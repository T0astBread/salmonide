package salmonide

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
)

const MethodPing = "ping"

const methodAuthenticate = "authenticate"

type paramsAuthenticate struct {
	Name string
	Type ActorType
}

type ActorType int

const (
	ActorTypeUnknown ActorType = iota
	ActorTypeCoordinator
	ActorTypeRunner
	ActorTypeWorker
)

type PeerID uint

type Peer struct {
	Authenticated bool
	ID            PeerID
	Name          string
	Type          ActorType
	Conn          *jsonrpc2.Conn

	ctx context.Context
}

type RequestHandler func(ctx context.Context, actor *Actor, sender PeerID, request *jsonrpc2.Request) (result interface{}, err error)

type PeerConnectedCallback func(ctx context.Context, peer Peer) error

type Actor struct {
	OnPeerConnected PeerConnectedCallback

	handleFn            RequestHandler
	name                string
	nextPeerID          PeerID
	peerDisconnectError map[PeerID]error
	peers               map[PeerID]Peer
	peersMutex          sync.RWMutex
	typ                 ActorType
}

func NewActor(name string, typ ActorType, handle RequestHandler) *Actor {
	return &Actor{
		handleFn:            handle,
		name:                name,
		peerDisconnectError: map[PeerID]error{},
		peers:               map[PeerID]Peer{},
		peersMutex:          sync.RWMutex{},
		typ:                 typ,
	}
}

func (a *Actor) ConnectPeer(ctx context.Context, transport io.ReadWriteCloser) (Peer, error) {
	return a.connectPeer(ctx, transport, true)
}

func (a *Actor) connectPeer(ctx context.Context, transport io.ReadWriteCloser, initiatedByUs bool) (Peer, error) {
	peerCtx, cancelConnectionCtx := context.WithCancel(ctx)

	peer := Peer{
		ID: a.nextPeerID,

		ctx: peerCtx,
	}
	a.nextPeerID++

	a.peersMutex.Lock()
	a.peers[peer.ID] = peer
	a.peersMutex.Unlock()

	handler := &ActorHandler{
		actor:     a,
		authDone:  make(chan struct{}),
		authError: make(chan error, 1),
		cancelConnection: func(err error) {
			a.peersMutex.Lock()
			a.peerDisconnectError[peer.ID] = err
			a.peersMutex.Unlock()
			cancelConnectionCtx()
		},
		ctx:                peerCtx,
		handledPeer:        peer.ID,
		initializationDone: make(chan struct{}),
		initiatedByUs:      initiatedByUs,
	}
	stream := jsonrpc2.NewBufferedStream(transport, jsonrpc2.PlainObjectCodec{})
	conn := jsonrpc2.NewConn(peerCtx, stream, handler)

	handler.conn = conn
	peer.Conn = conn

	a.peersMutex.Lock()
	a.peers[peer.ID] = peer
	a.peersMutex.Unlock()
	go func() {
		<-conn.DisconnectNotify()
		a.peersMutex.Lock()
		defer a.peersMutex.Unlock()
		delete(a.peers, peer.ID)
	}()

	close(handler.initializationDone)
	log.Println("opened peer connection")

	// pingCtx, cancelPingCtx := context.WithTimeout(peerCtx, 5*time.Second)
	// defer cancelPingCtx()
	// var pingResponse string
	// if err := conn.Call(pingCtx, MethodPing, nil, &pingResponse); err != nil {
	// 	log.Printf("error pinging peer: %s\n", err.Error())
	// 	handler.cancelConnection(err)
	// 	return Peer{}, err
	// }

	if initiatedByUs {
		if err := handler.authenticateToPeer(peerCtx); err != nil {
			log.Printf("error authenticating to peer: %s\n", err.Error())
			handler.cancelConnection(err)
			return Peer{}, err
		}

		log.Println("authenticated to peer")
	}

	authErr := <-handler.authError
	log.Println("peer authentication complete")
	return peer, authErr
}

func (a *Actor) FindPeer(id PeerID) (Peer, bool) {
	a.peersMutex.RLock()
	defer a.peersMutex.RUnlock()
	peer, ok := a.peers[id]
	return peer, ok
}

func (a *Actor) GetSender(id PeerID) (Peer, error) {
	peer, ok := a.FindPeer(id)
	if !ok {
		return peer, errors.New("could not find sender in peers map")
	}
	return peer, nil
}

func (a *Actor) GetDisconnectError(id PeerID) error {
	a.peersMutex.RLock()
	defer a.peersMutex.RUnlock()
	return a.peerDisconnectError[id]
}

type ActorHandler struct {
	actor              *Actor
	authDone           chan struct{}
	authError          chan error
	cancelConnection   func(error)
	conn               *jsonrpc2.Conn
	ctx                context.Context
	handledPeer        PeerID
	initializationDone chan struct{}
	initiatedByUs      bool
}

// Handle implements jsonrpc2.Handler
func (h *ActorHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, request *jsonrpc2.Request) {
	go h.handle(ctx, conn, request)
}

func (h *ActorHandler) handle(ctx context.Context, conn *jsonrpc2.Conn, request *jsonrpc2.Request) {
	<-h.initializationDone

	typeStr := "R"
	if request.Notif {
		typeStr = "N"
	}
	log.Printf("handling: %s %s %s", typeStr, request.ID.String(), request.Method)

	if request.Method == MethodPing {
		if replyErr := conn.Reply(ctx, request.ID, "pong"); replyErr != nil {
			log.Printf("an error occurred while answering the %s request: %s", request.Method, replyErr.Error())
		}
		return
	} else if request.Method == methodAuthenticate {
		err := h.handleAuthenticateRequest(ctx, h.handledPeer, request)
		defer func() {
			h.authError <- err
			close(h.authDone)
		}()
		if err != nil {
			log.Printf("error during authentication: %s\n", err.Error())
			errReply, ok := err.(*jsonrpc2.Error)
			if !ok {
				log.Printf("error while handling request %s (%s): %s\n", request.ID.String(), request.Method, err.Error())
				errReply = &jsonrpc2.Error{
					Code:    jsonrpc2.CodeInternalError,
					Message: err.Error(),
				}
			}
			if replyErr := conn.ReplyWithError(ctx, request.ID, errReply); replyErr != nil {
				log.Printf("an additional error occurred while answering the %s request: %s", request.Method, replyErr.Error())
			}
			h.cancelConnection(err)
			return
		}
		log.Println("authenticated peer")
		if replyErr := conn.Reply(ctx, request.ID, nil); replyErr != nil {
			log.Printf("an error occurred while answering the %s request: %s", request.Method, replyErr.Error())
			h.cancelConnection(err)
			return
		}
		return
	}

	<-h.authDone
	result, err := h.actor.handleFn(ctx, h.actor, h.handledPeer, request)

	if request.Notif {
		if err != nil {
			log.Printf("error while handling notification %s (%s): %s\n", request.ID.String(), request.Method, err.Error())
			h.cancelConnection(err)
		}
		return
	}

	replyErr := func() error {
		if err == nil {
			return conn.Reply(ctx, request.ID, result)
		}

		errReply, ok := err.(*jsonrpc2.Error)
		if !ok {
			log.Printf("error while handling request %s (%s): %s\n", request.ID.String(), request.Method, err.Error())
			errReply = &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInternalError,
				Message: err.Error(),
			}
		}
		return conn.ReplyWithError(ctx, request.ID, errReply)
	}()

	if replyErr != nil {
		log.Printf("error sending reply for request %s (%s): %s\n", request.ID.String(), request.Method, replyErr.Error())
		h.cancelConnection(replyErr)
	}
}

func (h *ActorHandler) handleAuthenticateRequest(ctx context.Context, sender PeerID, request *jsonrpc2.Request) error {
	var params paramsAuthenticate
	if err := json.Unmarshal(*request.Params, &params); err != nil {
		return err
	}

	h.actor.peersMutex.Lock()
	peer := h.actor.peers[sender]
	peer.Authenticated = true
	peer.Name = params.Name
	peer.Type = params.Type
	h.actor.peers[sender] = peer
	h.actor.peersMutex.Unlock()

	if !h.initiatedByUs {
		if err := h.authenticateToPeer(ctx); err != nil {
			return fmt.Errorf("error authenticating to peer: %w", err)
		}
	}

	cb := h.actor.OnPeerConnected
	if cb != nil {
		if err := cb(peer.ctx, peer); err != nil {
			h.handleAuthenticationError(err)
			return err
		}
	}

	return nil
}

func (h *ActorHandler) handleAuthenticationError(err error) {
	log.Printf("error during peer authentication: %s\n", err)
	h.cancelConnection(err)
}

func (handler *ActorHandler) authenticateToPeer(ctx context.Context) error {
	return handler.conn.Call(ctx, methodAuthenticate, paramsAuthenticate{
		Name: handler.actor.name,
		Type: handler.actor.typ,
	}, nil)
	// if err := handler.conn.Call(ctx, methodAuthenticate, paramsAuthenticate{
	// 	Name: handler.actor.name,
	// 	Type: handler.actor.typ,
	// }, nil); err != nil {
	// 	log.Printf("error authenticating to peer: %s\n", err.Error())
	// 	handler.cancelConnection(err)
	// 	return err
	// }

	// log.Println("authenticated to peer")
	// return nil
}

var _ jsonrpc2.Handler = (*ActorHandler)(nil)

func ListenNet(ctx context.Context, listener net.Listener, actor *Actor) error {
	cancelContext, cancelConnections := context.WithCancel(ctx)
	defer cancelConnections()

	for {
		transport, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("error accepting connection: %s\n", err.Error())
				return err
			}
			return nil
		}
		defer transport.Close()

		go func() {
			peer, err := actor.connectPeer(cancelContext, transport, false)
			if err == nil {
				<-peer.ctx.Done()
			}
			_ = transport.Close()
		}()
	}
}

func ParseParams[T any](request *jsonrpc2.Request) (T, error) {
	var params T
	if err := json.Unmarshal(*request.Params, &params); err != nil {
		return params, &jsonrpc2.Error{
			Code: jsonrpc2.CodeInvalidParams,
		}
	}
	return params, nil
}

func MustBeNotification(request *jsonrpc2.Request) error {
	if !request.Notif {
		return &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: fmt.Sprintf("%s must be a notification but got a request", request.Method),
		}
	}
	return nil
}

func MustBeRequest(request *jsonrpc2.Request) error {
	if request.Notif {
		return fmt.Errorf("%s must be a request but got a notification", request.Method)
	}
	return nil
}

func NewMethodNotFoundError(method string) *jsonrpc2.Error {
	return &jsonrpc2.Error{
		Code:    jsonrpc2.CodeMethodNotFound,
		Message: method,
	}
}

func combinedCtx(ctx1, ctx2 context.Context) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx1.Done():
		case <-ctx2.Done():
		}
		cancel()
	}()
	return ctx
}
