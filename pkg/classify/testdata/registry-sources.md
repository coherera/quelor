# Package-registry fixture sources

The positive registry rows in `gha_signatures.json` are excerpts of publicly
reported CI failures, retrieved on 2026-11-18. They contain no credentials or
private paths. Only the diagnostic lines needed for classification are retained;
HTTP status is deliberately absent so the tests exercise log matching.

| Fixture | Public log source | Extraction |
| --- | --- | --- |
| Maven Central 429 | [NVIDIA/cuopt #1820](https://github.com/NVIDIA/cuopt/issues/1820), reporting [java-build job 98682899970](https://github.com/NVIDIA/cuopt/actions/runs/33117050967/job/98682899970) | The continuation line containing the registry URL and response status, unchanged. |
| Gradle registry 429 | [archived Apache Grails CI failure report](https://www.mail-archive.com/notifications@grails.apache.org/msg27971.html) (GitHub comment 6680466742), for [Build Grails Forge job 127762363886](https://github.com/apache/grails-core/actions/runs/42178411138/job/127762363886) | The `Received status code` line; leading indentation normalized. |
| npm registry E429 | [npm/cli #8507](https://github.com/npm/cli/issues/8507), reporting [GitHub Actions job 47718853834](https://github.com/vezham/heroui/actions/runs/16843420087/job/47718853834) | The `npm error code E429` line, unchanged; the following response text is omitted to isolate the previously unmatched error code. |
| npm legacy registry E429 | [tomchen/example-typescript-package #8](https://github.com/tomchen/example-typescript-package/issues/8), reporting [publish job 9010098391](https://github.com/RoyLeibo/phone-number-generator/actions/runs/5024408147/jobs/9010098391) | The `npm ERR! code E429` line, unchanged. |

The 403, authentication, quoted-error and identifier rows are synthetic negative
controls, not claimed as observed CI logs. A bare 403 is not evidence of
throttling. These fixtures do not provoke live registry limits.

The [registry circuit example](../../../examples/manifests/registry-circuit.yaml)
requires resource-scoped HTTP observations. Log classification alone does not
update the engine's circuit state, and the example does not infer an unreported
registry budget.
