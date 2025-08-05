# Changelog - Core Library

All notable changes to the core kafka-pulse-go library will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: Adapters have their own changelogs:
- [Sarama Adapter](adapter/sarama/CHANGELOG.md)
- [Confluent Kafka Go Adapter](adapter/confluentic/CHANGELOG.md)
- [SegmentIO Kafka Go Adapter](adapter/segmentio/CHANGELOG.md)

## [Unreleased]

### Added

### Changed

### Fixed

## [0.0.1] - 2025-08-05

### Added
- Core kafka-pulse-go library with Monitor interface
- HealthChecker component for consumer lag detection
- Stuck consumer detection logic (distinguishes stuck vs idle consumers)
- Project template structure and development tooling
- Task automation with Taskfile.yml
- GitHub Actions CI workflow