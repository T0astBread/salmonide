with (import <nixpkgs> { });

mkShell {
  packages = [
    delve
    go_1_18
    nixos-generators
    qemu
    sqlitebrowser
  ];

  shellHook = "\${NIX_SHELL_GLOBAL_HOOK:-}";
}
