{
  description = "WhatsApp MCP server, packaged as a cattle container";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  inputs.cattle.url = "git+https://github.com/charlesbaynham/nix-proxmox-cattle?ref=v1";

  outputs = { self, nixpkgs, cattle }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # go.mod wants go >= 1.26.8; this pin's default `go` is 1.26.7.
      bridge = (pkgs.buildGoModule.override { go = pkgs.go_1_27; }) {
        pname = "whatsapp-bridge";
        version = "0.1.0";
        src = ./whatsapp-bridge;
        vendorHash = "sha256-8yTDqljzX2N69Q+GHA3BI8FXpR0nhR3N6ke1UFYPp6g=";
        env.CGO_ENABLED = "1"; # go-sqlite3 is cgo.
      };

      # nixos-26.05 already carries mcp 1.26.0 (>=1.10,<2), so no overlay.
      mcpEnv = pkgs.python3.withPackages (ps: [ ps.mcp ps.requests ps.httpx ]);
    in
    cattle.lib.mkTemplate {
      inherit nixpkgs system;
      name = "whatsapp";
      stateDir = "/data";
      modules = [
        ./nix/whatsapp.nix
        {
          services.whatsapp = {
            enable = true;
            inherit bridge mcpEnv;
            mcpSource = ./whatsapp-mcp-server;
          };
        }
      ];
    };
}
