# Third-party notices

The project code is MIT licensed. The Phoenix broker's exact dependency versions are pinned in `broker/mix.lock`. Phoenix, Plug, Bandit, Jason and their dependencies retain their upstream terms; Gun/Cowlib and JOSE also retain their own licenses. Do not apply this repository's MIT license to those components.

`broker/scripts/package_notices.exs` copies each dependency's upstream LICENSE/NOTICE/COPYING files into the production image at `/app/licenses/`, together with `mix.lock` and the project's MIT text. A missing dependency license file stops the build. `broker/LICENSE` mirrors the root LICENSE because the broker is a separate Docker build context.

Nginx, Elixir/Erlang, Alpine and system libraries are separate distributions. Preserve their included notices and satisfy any source obligations if distributing a combined image. The broker's collected notices are not a complete operating-system SBOM.
