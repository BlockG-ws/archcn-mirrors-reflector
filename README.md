# Reflector for [archlinuxcn]
This repo generates mirror info from archlinuxcn's [mirrorlist](https://github.com/archlinuxcn/mirrorlist-repo) and can be used with reflector
## Usage
Due to the limitation for `reflector`, you can't use reflector with mirrors.json directly, but it can be solved using the wrapper:

```bash
curl -OL https://github.com/BlockG-ws/archcn-mirrors-reflector/raw/refs/heads/master/reflector-archlinuxcn
./reflector-archlinuxcn --url https://example.org/mirrors.json \\
      --country CN --sort score --latest 5 --save /etc/pacman.d/archlinuxcn-mirrorlist # just replace the `reflector` command and use like before
```
## Known Problems
Due to the limitation of intervals for action execution, the JSON can't always reflect the latest status.
## License
[GNU General Public License v3.0](./COPYING)
