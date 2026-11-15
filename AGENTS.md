# Repository guidance

## Shared conventions

Follow the existing architecture, naming and coding style. Preserve public contracts, authentication, certificate verification and project policy. Keep changes in the component that owns the behavior.

Update user documentation and examples when behavior changes. Run the relevant checks; place regression tests with the owning component. Keep secrets, execution logs, investigation notes and standalone upstream reproductions outside the repository.

## Fork-specific guidance

Keep the upstream README, attribution and contribution policy intact. Limit rstream additions to the reason for retaining the fork and the constraints in `RSTREAM_FORK.md`. Preserve its owning regression tests when updating upstream source.
