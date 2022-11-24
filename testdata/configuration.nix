{ config, lib, pkgs, ... }:

let
  worker = pkgs.buildGoModule.override
    {
      go = pkgs.go_1_18;
      stdenv = pkgs.stdenvAdapters.keepDebugInfo pkgs.stdenv;
    }
    rec {
      name = "salmonide-worker";

      src = ../.;
      vendorSha256 = "sha256-qvBeQBwnVFjSMElS4aJ0ZmjvZuJAeWM7HOBRDaZx2i0=";

      subPackages = [ "worker/cmd" ];

      postFixup = ''
        mv $out/bin/cmd $out/bin/${name}
      '';
    };
in
{
  users.users.root.password = "nixos";
  services.openssh.permitRootLogin = lib.mkDefault "yes";
  services.getty.autologinUser = lib.mkDefault "root";

  time.timeZone = "Europe/Vienna";

  i18n.defaultLocale = "en_US.UTF-8";
  console = {
    font = "Lat2-Terminus16";
    keyMap = "de";
  };

  environment.systemPackages = [ worker pkgs.delve pkgs.go_1_18 ];

  systemd.services.salmonide-worker = {
    enable = true;
    path = [ pkgs.bash ];
    script = ''
      ${worker}/bin/salmonide-worker
    '';
    wants = [ "network-online.target" ];
    after = [ "network-online.target" ];
    wantedBy = [ "default.target" ];
  };

  system.stateVersion = "22.05";
}
