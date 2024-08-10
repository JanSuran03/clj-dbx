# clj-dbx
A Clojure library for generating inline SQL queries.

## Why?

JDBC supports batch execution of a single prepared statement - but what if you wanted to prepare
and execute more different statements at once?

## Usage
- TODO: update
- inlining keyword arguments inside queries (e.g. `DELETE FROM users WHERE id = :id`) will simply
treat the keyword arguments as Clojure map entries - you can parse this template with
`clj-dbx.dsl-parser/parse-query` and then fil in the arguments with `clj-dbx.sql-builder/build`.
- result set parsing... TODO doc - I'm too lazy

## License

Copyright © 2024 Jan Šuráň

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
