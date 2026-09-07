{
  description = "WhatsApp MCP server, packaged as a cattle container";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  inputs.cattle.url = "git+https://github.com/charlesbaynham/nix-proxmox-cattle?ref=v1";

  outputs = { self, nixpkgs, cattle }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # go-sqlite3 is cgo; CGO_ENABLED is off by default under buildGoModule's
      # cross-compilation defaults, which would otherwise fail at link time
      # with an unhelpful "undefined reference" rather than a clear error.
      #
      # go.mod's `go` directive outgrows this nixpkgs pin's default toolchain
      # (1.26.7) the moment the hosted-service branch merges (it moves to
      # 1.26.8, for the whatsmeow bump). go_1_27 is 1.27.1, already in this
      # same pin, so overriding it needs no second nixpkgs input and satisfies
      # both today's directive and the incoming one.
      bridge = (pkgs.buildGoModule.override { go = pkgs.go_1_27; }) {
        pname = "whatsapp-bridge";
        version = "0.1.0";
        src = ./whatsapp-bridge;
        vendorHash = "sha256-8yTDqljzX2N69Q+GHA3BI8FXpR0nhR3N6ke1UFYPp6g=";
        env.CGO_ENABLED = "1";
      };

      # nixos-26.05 carries mcp 1.26.0: past the 1.10 floor streamable-http
      # needs, and below 2.0, which renames FastMCP - so no overlay is needed.
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
