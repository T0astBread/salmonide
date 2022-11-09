with (import <nixpkgs> { });

mkShell {
  packages = [
    go_1_18
    nixos-generators
    qemu
    sqlitebrowser
  ];
}
