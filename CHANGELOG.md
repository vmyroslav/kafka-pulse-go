# Changelog - Core Library

All notable changes to the core kafka-pulse-go library will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: Adapters have their own changelogs:
- [Sarama Adapter](adapter/sarama/CHANGELOG.md)
- [Confluent Kafka Go Adapter](adapter/confluentic/CHANGELOG.md)
- [SegmentIO Kafka Go Adapter](adapter/segmentio/CHANGELOG.md)

## [Unreleased]

## [v2.0.0] - 2025-07-24

### Added
- Independent versioning system for core library and adapters
- Component-specific release tasks and changelog management
- Multi-component GitHub Actions workflow
- Automatic change detection for release preparation

## [v1.1.0] - 2025-07-13

### Added
- GitHub issue templates for bug reports and feature requests
- GitHub pull request template with comprehensive checklist

## [v1.0.0] - 2025-07-01

### Added
- Core kafka-pulse-go library with Monitor interface
- HealthChecker component for consumer lag detection
- Thread-safe offset tracker with timestamps
- TrackableMessage and BrokerClient interfaces
- Stuck consumer detection logic (distinguishes stuck vs idle consumers)
- Project template structure and development tooling
- Task automation with Taskfile.yml
- GitHub Actions CI workflow
- Claude Code guidance documentation (CLAUDE.md)

### Changed

### Deprecated

### Removed

### Fixed

### Security