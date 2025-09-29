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
#   $ vhs vhs/resume-after-cancel.tape
#
# More info: https://github.com/charmbracelet/vhs

Output resume-after-cancel.gif

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 900

Type "go run -tags wip . large-upload-demo"
Enter
Sleep 5s
Ctrl+C
Sleep 1s

Type "go run -tags wip . large-upload-demo --resume"
Enter
Wait@60s
Sleep 2s
