# To generate:
#
#   $ brew install vhs          # macOS
#   $ pacman -S vhs             # Arch Linux
#   $ nix-env -iA nixpkgs.vhs   # NixOS
#
# or even:
#
#   $ go install github.com/charmbracelet/vhs@latest
#
# Then run:
#
#   $ vhs vhs/resume-after-read-error.tape
#
# More info: https://github.com/charmbracelet/vhs

Output success.gif

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 900

Type "go run -tags wip . large-upload-demo"
Enter
Wait@60s
Sleep 2s