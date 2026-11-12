# unixODBC

The Linux and macOS wheels of this package include the unixODBC driver manager library
(`libodbc`), version 2.3.14, unmodified. Windows wheels include no unixODBC; they use the
driver manager built into Windows (`odbc32.dll`).

- Licence: GNU Lesser General Public License, version 2.1; its text is in `unixODBC-COPYING.txt`
  next to this file.
- Corresponding source: `unixODBC-2.3.14.tar.gz`, SHA-256
  4e2814de3e01fc30b0b9f75e83bb5aba91ab0384ee951286504bb70205524771, is included in this package's
  source distribution (sdist) of the same version on PyPI, at its top level. It is unixODBC's own
  release, also at https://www.unixodbc.org/unixODBC-2.3.14.tar.gz and
  https://github.com/lurcher/unixODBC/releases/tag/v2.3.14.
- Built by `.github/scripts/build_unixodbc.sh` in https://github.com/sqlstack-io/sqlload, with
  `--with-included-ltdl`, the configuration folder set to `/etc` on Linux and Homebrew's `etc` on
  macOS, and drivers registered by file name alone looked for in
  `/usr/lib/<arch>-linux-gnu/odbc`, `/usr/lib/<arch>-linux-gnu`, `/usr/lib64/odbc` and `/usr/lib64`
  on Linux and Homebrew's `lib` on macOS.

SQLLoad links the library dynamically. It is installed as a separate shared library next to the
package (`*.libs/libodbc-*.so.2` on Linux, `*.dylibs/libodbc-*.2.dylib` on macOS); you may
replace that file with another build of unixODBC 2.3 with the same interface.
