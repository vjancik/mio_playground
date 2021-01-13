### mio_playground

Exploratory project to determine pure `mio` (as opposed to `tokio`) runtime viability for larger scale projects.

#### Design decisions
 - Event sources aren't registered across threads after the runtime starts.
 - Thread data is thread local after the runtime starts.