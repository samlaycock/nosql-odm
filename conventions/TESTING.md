Run the unit tests with `bun run test`
Run the integration tests for each query engine with `bun run services:up:<query-engine> && bun run test:integration:<query-engine>`
Clean up the integration test environment with `bun run services:down:<query-engine>`
Always write unit tests for all new functionality.
Make sure to mock timeouts and intervals in tests.
Use descriptive test names.
Group related tests using describe blocks.
Aim for high code coverage.
Avoid duplication of testing scenarios.
