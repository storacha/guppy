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
#   $ vhs vhs/resume-after-dag-error.tape
#
# More info: https://github.com/charmbracelet/vhs

Output resume-after-dag-error.gif

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 900

Type "go run -tags wip . upload-demo --alter-metadata"
Enter
Wait+Screen /Upload failed due to out-of-date scan:/

Type "go run -tags wip . upload-demo --resume"
Enter
Wait@60s
Sleep 2s
