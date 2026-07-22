<!--
Thanks for contributing! Keep commits as plain conventional-commit messages
(feat:/fix:/build:/docs:/test:/chore:) with no AI-attribution or session
trailers, per CLAUDE.md.
-->

## Summary

<!-- What does this PR do, and why? -->

Closes #

## Changes

<!-- Bullet the notable changes. -->
-

## Testing

<!-- How was this verified? Tick what applies. -->
- [ ] `go build -tags goolm ./...`
- [ ] `go vet -tags goolm ./...`
- [ ] `go test -tags goolm ./...`
- [ ] Live-verified against a real Google Chat account (required for any new
      outbound RPC / endpoint — describe what you ran and saw)

## Notes for the reviewer

<!--
- Any unverified Google Chat endpoints or behaviour still to confirm?
- Layering respected? (pkg/gchatmeow never imports bridgev2; pkg/connector
  never does HTTP; pkg/gcid formats are frozen.)
- Does this touch the ROADMAP feature matrix? Update ROADMAP.md if so.
-->
