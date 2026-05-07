# Contributing to Sparkling

Thanks for your interest in contributing! Here's how to get started.

## Building and Testing

See the [Building and Testing](README.md#building-and-testing) section in the README for setup instructions and available sbt commands.

Before submitting a pull request, make sure the full CI check passes locally:

```
sbt ci
```

This runs linting, formatting checks, cross-build compilation, and tests with coverage for both Scala 2.12 and 2.13.

If your changes affect public API or Scaladoc comments, also run:

```
sbt +doc
```

This is the only way to catch broken class links, unresolvable references, and other Scaladoc issues that `sbt ci` does not cover. Check the output for warnings before submitting.

## Code Style

Formatting and import ordering are enforced automatically by [Scalafmt](https://scalameta.org/scalafmt/) and [Scalafix](https://scalacenter.github.io/scalafix/). Run the following before committing:

```
sbt formatAll
```

## Submitting a Pull Request

1. Fork the repository and create a branch from `main`
2. Make your changes, including tests for any new behavior
3. Run `sbt ci` to verify everything passes
4. Open a pull request against `main` with a clear description of what changed and why

For larger changes, consider opening an issue first to discuss the approach before investing time in an implementation.

## Reporting Issues

Please use the [issue tracker](https://github.com/andy327/sparkling/issues) to report bugs or request features. See the issue templates for guidance on what to include.
