package salmonide

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
)

const (
	MethodPing = "ping"

	methodAuthenticate = "authenticate"
)

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

type msgAuthenticateToPeer struct {
	id PeerID
}

type msgConnectPeer struct {
	initiatedByUs bool
	onDisconnect  func()
	transport     io.ReadWriteCloser
}

type msgDisconnectPeer struct {
	id     PeerID
	reason error
}

type msgExit struct {
	err error
}

type msgPeerAuthenticationComplete struct {
	id PeerID
}

type msgRPC struct {
	ctx     context.Context
	sender  PeerID
	request *jsonrpc2.Request
}

type PeerID uint

type Peer struct {
	AuthFromThem  bool
	AuthFromUs    bool
	Conn          *jsonrpc2.Conn
	Ctx           context.Context
	ID            PeerID
	InitiatedByUs bool
	Logger        *log.Logger
	Name          string
	Type          ActorType

	cancelCtx context.CancelFunc
}

type ConnectHandler func(ctx context.Context, peer *Peer) error
type CustomMessageHandler func(ctx context.Context, msg interface{}) error
type DisconnectHandler func(ctx context.Context, peer *Peer) error
type RequestHandler func(ctx context.Context, sender *Peer, request *jsonrpc2.Request) (result interface{}, err error)

type Actor struct {
	Logger *log.Logger

	handleConnect    ConnectHandler
	handleCustomMsg  CustomMessageHandler
	handleDisconnect DisconnectHandler
	handleRequest    RequestHandler
	msgChan          chan interface{}
	name             string
	nextPeerID       PeerID
	peers            map[PeerID]*Peer
	typ              ActorType
}

func NewActor(logWriter io.Writer, name string, typ ActorType, handleConnect ConnectHandler, handleCustomMsg CustomMessageHandler, handleDisconnect DisconnectHandler, handleRequest RequestHandler) *Actor {
	return &Actor{
		Logger: log.New(logWriter, name+" | ", log.LstdFlags),

		handleConnect:    handleConnect,
		handleCustomMsg:  handleCustomMsg,
		handleDisconnect: handleDisconnect,
		handleRequest:    handleRequest,
		msgChan:          make(chan interface{}),
		name:             name,
		peers:            map[PeerID]*Peer{},
		typ:              typ,
	}
}

func (a *Actor) ConnectPeer(ctx context.Context, transport io.ReadWriteCloser, onDisconnect func()) {
	a.Message(msgConnectPeer{
		initiatedByUs: true,
		onDisconnect:  onDisconnect,
		transport:     transport,
	})
}

func (a *Actor) Exit(err error) {
	a.Message(msgExit{
		err: err,
	})
}

func (a *Actor) Message(msg interface{}) {
	a.Logger.Printf("posting message %#v", msg)
	a.msgChan <- msg
	a.Logger.Printf("posted message %#v", msg)
}

func (a *Actor) ProcessMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-a.msgChan:
			if msg, ok := msg.(msgExit); ok {
				return msg.err
			}
			if err := a.handleMsg(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (a *Actor) handleMsg(ctx context.Context, msg interface{}) error {
	switch msg := msg.(type) {
	case msgAuthenticateToPeer:
		return a.handleMsgAuthenticateToPeer(ctx, msg)
	case msgConnectPeer:
		return a.handleMsgConnectPeer(ctx, msg)
	case msgDisconnectPeer:
		return a.handleMsgDisconnectPeer(ctx, msg)
	case msgPeerAuthenticationComplete:
		return a.handleMsgPeerAuthenticationComplete(ctx, msg)
	case msgRPC:
		return a.handleMsgRPC(ctx, msg)
	default:
		if a.handleCustomMsg == nil {
			return a.NewErrUnknownMessage(msg)
		}
		return a.handleCustomMsg(ctx, msg)
	}
}

func (a *Actor) handleMsgAuthenticateToPeer(ctx context.Context, msg msgAuthenticateToPeer) error {
	peer := a.peers[msg.id]
	if err := peer.Conn.Call(ctx, methodAuthenticate, paramsAuthenticate{
		Name: a.name,
		Type: a.typ,
	}, nil); err != nil {
		go a.Message(msgDisconnectPeer{
			id:     peer.ID,
			reason: fmt.Errorf("error authenticating to peer %d: %w", peer.ID, err),
		})
		return nil
	}
	peer.AuthFromUs = true

	if a.handleConnect != nil {
		if err := a.handleConnect(ctx, peer); err != nil {
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: fmt.Errorf("error during connect callback for peer %d: %w", peer.ID, err),
			})
		}
	}

	return nil
}

func (a *Actor) handleMsgConnectPeer(ctx context.Context, msg msgConnectPeer) error {
	peerCtx, cancelPeerCtx := context.WithCancel(context.Background())
	peerID := a.nextPeerID
	a.nextPeerID++

	if msg.onDisconnect != nil {
		go func() {
			<-peerCtx.Done()
			msg.onDisconnect()
		}()
	}

	handler := actorHandler{
		actor:       a,
		handledPeer: peerID,
	}

	peerLogger := log.New(a.Logger.Writer(), fmt.Sprintf("%s->#%d | ", a.name, peerID), log.LstdFlags)

	stream := jsonrpc2.NewBufferedStream(msg.transport, jsonrpc2.VarintObjectCodec{})
	conn := jsonrpc2.NewConn(peerCtx, stream, &handler, jsonrpc2.OnSend(func(req *jsonrpc2.Request, _ *jsonrpc2.Response) {
		if req == nil {
			return
		}

		if req.Notif {
			peerLogger.Printf("sending notification for \"%s\": %s", req.Method, req.Params)
		} else {
			peerLogger.Printf("sending request %s for \"%s\": %s", req.ID.String(), req.Method, req.Params)
		}
	}), jsonrpc2.OnRecv(func(req *jsonrpc2.Request, res *jsonrpc2.Response) {
		if res == nil {
			return
		}

		if res.Error == nil {
			peerLogger.Printf("received response for request %s: %s", req.ID.String(), *res.Result)
		} else {
			peerLogger.Printf("received error response for request %s: %s", req.ID.String(), res.Error.Error())
		}
	}))

	peer := &Peer{
		cancelCtx:     cancelPeerCtx,
		Conn:          conn,
		Ctx:           peerCtx,
		ID:            peerID,
		InitiatedByUs: msg.initiatedByUs,
		Logger:        peerLogger,
	}

	a.peers[peerID] = peer

	go func() {
		<-conn.DisconnectNotify()
		a.Message(msgDisconnectPeer{
			id: peerID,
		})
	}()

	if msg.initiatedByUs {
		if err := peer.Conn.Call(ctx, methodAuthenticate, paramsAuthenticate{
			Name: a.name,
			Type: a.typ,
		}, nil); err != nil {
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: fmt.Errorf("error authenticating to peer %d: %w", peer.ID, err),
			})
			return nil
		}
		peer.AuthFromUs = true
	}

	return nil
}

func (a *Actor) handleMsgDisconnectPeer(ctx context.Context, msg msgDisconnectPeer) error {
	peer, ok := a.peers[msg.id]
	if !ok {
		return nil
	}

	var err error
	if a.handleDisconnect != nil {
		err = a.handleDisconnect(ctx, peer)
		if err != nil {
			a.Logger.Println("error during disconnect callback:", err.Error())
		}
	}

	peer.cancelCtx()
	delete(a.peers, msg.id)

	if msg.reason == nil {
		peer.Logger.Println("disconnecting")
	} else {
		peer.Logger.Println("disconnecting; reason:", msg.reason.Error())
	}

	return err
}

func (a *Actor) handleMsgPeerAuthenticationComplete(ctx context.Context, msg msgPeerAuthenticationComplete) error {
	peer, ok := a.peers[msg.id]
	if !ok {
		a.Logger.Printf("couldn't find peer %d to update authentication status; maybe disconnected?\n", msg.id)
		return nil
	}

	if !peer.InitiatedByUs {
		peer.AuthFromUs = true
	} else if a.handleConnect != nil {
		if err := a.handleConnect(ctx, peer); err != nil {
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: fmt.Errorf("error during connect callback for peer %d: %w", peer.ID, err),
			})
		}
	}

	return nil
}

func (a *Actor) handleMsgRPC(msgProcessingCtx context.Context, msg msgRPC) error {
	peer := a.peers[msg.sender]
	ctx := CombinedCtx(CombinedCtx(msg.ctx, peer.Ctx), msgProcessingCtx)

	if msg.request.Notif {
		peer.Logger.Printf("got notif for \"%s\": %s", msg.request.Method, *msg.request.Params)
	} else {
		peer.Logger.Printf("got request %s for \"%s\": %s", msg.request.ID.String(), msg.request.Method, *msg.request.Params)
	}

	var result interface{}
	var err error
	switch msg.request.Method {
	case methodAuthenticate:
		result, err = a.handleRequestAuthenticate(ctx, peer, msg.request)
	case MethodPing:
		result, err = a.handleRequestPing(ctx, peer, msg.request)
	default:
		result, err = a.handleOtherRequest(ctx, peer, msg.request)
	}

	if err != nil {
		if msg.request.Notif {
			err := fmt.Errorf("error while handling notification \"%s\": %w", msg.request.Method, err)
			peer.Logger.Println(err.Error())
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: err,
			})
			return nil
		}

		errorReply, ok := err.(*jsonrpc2.Error)
		if !ok {
			errorReply = &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInternalError,
				Message: err.Error(),
			}
		}
		peer.Logger.Printf("replying with error to request %s: %s", msg.request.ID.String(), errorReply.Error())
		if err := peer.Conn.ReplyWithError(ctx, msg.request.ID, errorReply); err != nil {
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: fmt.Errorf("error while sending error reply for request %s: %w", msg.request.ID.String(), err),
			})
		}
		return nil
	}

	if !msg.request.Notif {
		peer.Logger.Printf("replying to request %s: %#v", msg.request.ID.String(), result)
		if err := peer.Conn.Reply(ctx, msg.request.ID, result); err != nil {
			go a.Message(msgDisconnectPeer{
				id:     peer.ID,
				reason: fmt.Errorf("error while sending reply for request %s: %w", msg.request.ID.String(), err),
			})
		}
		return nil
	}

	return nil
}

func (a *Actor) handleRequestAuthenticate(ctx context.Context, peer *Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := MustBeRequest(request); err != nil {
		return nil, err
	}

	params, err := ParseParams[paramsAuthenticate](request)
	if err != nil {
		return nil, err
	}

	peer.Name = params.Name
	peer.Type = params.Type
	peer.AuthFromThem = true
	peer.Logger.SetPrefix(fmt.Sprintf("%s->%s | ", a.name, peer.Name))

	if peer.InitiatedByUs {
		go a.Message(msgPeerAuthenticationComplete{
			id: peer.ID,
		})
	} else {
		go a.Message(msgAuthenticateToPeer{
			id: peer.ID,
		})
	}

	return nil, nil
}

func (a *Actor) handleRequestPing(ctx context.Context, peer *Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := MustBeRequest(request); err != nil {
		return nil, err
	}

	return "pong", nil
}

func (a *Actor) handleOtherRequest(ctx context.Context, peer *Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if !(peer.AuthFromThem && peer.AuthFromUs) {
		return nil, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: "not authenticated",
		}
	}

	return a.handleRequest(ctx, peer, request)
}

func (a *Actor) NewErrUnknownMessage(msg interface{}) error {
	return fmt.Errorf("got message of unknown type in actor \"%s\": %#v", a.name, msg)
}

func (a *Actor) FindPeersByType(typ ActorType) (peers []*Peer) {
	for _, peer := range a.peers {
		if peer.Type == typ {
			peers = append(peers, peer)
		}
	}
	return peers
}

func (a *Actor) Close() {
	close(a.msgChan)
}

type actorHandler struct {
	actor       *Actor
	handledPeer PeerID
}

// Handle implements jsonrpc2.Handler
func (h *actorHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, request *jsonrpc2.Request) {
	h.actor.Message(msgRPC{
		ctx:     ctx,
		sender:  h.handledPeer,
		request: request,
	})
}

var _ jsonrpc2.Handler = (*actorHandler)(nil)

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

func ParseParams[T any](request *jsonrpc2.Request) (T, error) {
	var params T
	if err := json.Unmarshal(*request.Params, &params); err != nil {
		return params, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidParams,
			Message: err.Error(),
		}
	}
	return params, nil
}

func CombinedCtx(ctx1, ctx2 context.Context) context.Context {
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

func ListenNet(ctx context.Context, listener net.Listener, actor *Actor) error {
	connections := sync.WaitGroup{}
	defer connections.Wait()

	for {
		transport, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				actor.Logger.Printf("error accepting connection: %s\n", err.Error())
				return err
			}
			return nil
		}
		defer transport.Close()

		connections.Add(1)
		actor.Message(msgConnectPeer{
			onDisconnect: connections.Done,
			transport:    transport,
		})
	}
}
