# Third-party notices

The project code is MIT licensed. The Phoenix broker's exact dependency versions are pinned in `broker/mix.lock`. Phoenix, Plug, Bandit, Jason and their dependencies retain their upstream terms; Gun/Cowlib and JOSE also retain their own licenses. Do not apply this repository's MIT license to those components.

`broker/scripts/package_notices.exs` copies each dependency's upstream LICENSE/NOTICE/COPYING files into the production image at `/app/licenses/`, together with `mix.lock` and the project's MIT text. A missing dependency license file stops the build. `broker/LICENSE` mirrors the root LICENSE because the broker is a separate Docker build context.

Nginx, Elixir/Erlang, Alpine and system libraries are separate distributions. Preserve their included notices and satisfy any source obligations if distributing a combined image. The broker's collected notices are not a complete operating-system SBOM.

## Storage broker

`docker-compose.storage-broker.yml` runs the official `haproxy:3.2-alpine` image, pinned by digest and used unmodified. HAProxy is GPL-2.0-or-later (with an LGPL-2.1 exception for its API headers). Running the image does not convey it; if a modified HAProxy or a combined image is distributed, provide the corresponding source and keep the image's notices.

## Media relay

Wall deploys `coturn/coturn:4.7.0`, pinned by image digest in `docker-compose.media.yml`. Coturn uses the BSD-3-Clause license; its pinned upstream notice is retained in `media/COTURN-LICENSE`. The container also includes its upstream operating-system and OpenSSL components under their own licenses.
