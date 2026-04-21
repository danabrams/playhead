// MetadataActivationOverrideTests.swift
// playhead-8em9 (narL): Tests for the debug-scoped activation override.
//
// Covers:
//   - Debug builds: programmatic override flips the resolved config.
//   - Debug builds: override can be reset.
//   - Release-lock simulation: `MetadataActivationConfig.resolved` short-circuits
//     to `.default` when the compile-time release lock is active. The override
//     system is DEBUG-only by design; the lock is simulated here because release
//     builds cannot run XCTest.

import Foundation
import Testing
@testable import Playhead

@Suite("MetadataActivationConfig — Debug Override", .serialized)
struct MetadataActivationOverrideTests {

    // Each test must leave the override store clean.
    private func withCleanOverride<T>(_ body: () throws -> T) rethrows -> T {
        MetadataActivationOverride.reset()
        defer { MetadataActivationOverride.reset() }
        return try body()
    }

    @Test("No override set → resolved() returns .default")
    func resolvedWithoutOverrideIsDefault() {
        withCleanOverride {
            let resolved = MetadataActivationConfig.resolved()
            #expect(resolved == .default)
            #expect(!resolved.isLexicalInjectionActive)
            #expect(!resolved.isClassifierPriorShiftActive)
            #expect(!resolved.isFMSchedulingActive)
        }
    }

    #if DEBUG
    @Test("DEBUG build: programmatic override to .allEnabled flips resolved()")
    func debugOverrideAllEnabled() {
        withCleanOverride {
            MetadataActivationOverride.set(.allEnabled)
            let resolved = MetadataActivationConfig.resolved()
            #expect(resolved == .allEnabled)
            #expect(resolved.isLexicalInjectionActive)
            #expect(resolved.isClassifierPriorShiftActive)
            #expect(resolved.isFMSchedulingActive)
        }
    }

    @Test("DEBUG build: reset() clears the override")
    func debugOverrideReset() {
        withCleanOverride {
            MetadataActivationOverride.set(.allEnabled)
            MetadataActivationOverride.reset()
            let resolved = MetadataActivationConfig.resolved()
            #expect(resolved == .default)
        }
    }

    @Test("DEBUG build: release-lock env variable forces .default even with override set")
    func releaseLockIgnoresOverride() {
        withCleanOverride {
            MetadataActivationOverride.set(.allEnabled)
            // Simulate the release-build behavior: when the lock is set,
            // the resolver must return `.default` unconditionally.
            let resolved = MetadataActivationConfig.resolved(releaseLockActive: true)
            #expect(resolved == .default,
                    "Release-lock must ignore DEBUG override and force .default")
        }
    }

    @Test("DEBUG build: launch argument -MetadataActivationOverride allEnabled sets override")
    func launchArgumentAllEnabled() {
        withCleanOverride {
            let args = ["Playhead", "-MetadataActivationOverride", "allEnabled"]
            MetadataActivationOverride.applyLaunchArguments(args)
            let resolved = MetadataActivationConfig.resolved()
            #expect(resolved == .allEnabled)
        }
    }

    @Test("DEBUG build: launch argument default → explicit .default (clears override)")
    func launchArgumentExplicitDefault() {
        withCleanOverride {
            MetadataActivationOverride.set(.allEnabled)
            let args = ["Playhead", "-MetadataActivationOverride", "default"]
            MetadataActivationOverride.applyLaunchArguments(args)
            #expect(MetadataActivationConfig.resolved() == .default)
        }
    }

    @Test("DEBUG build: unknown launch argument value is ignored (no-op)")
    func launchArgumentUnknownIgnored() {
        withCleanOverride {
            MetadataActivationOverride.set(.allEnabled)
            let args = ["Playhead", "-MetadataActivationOverride", "garbage"]
            MetadataActivationOverride.applyLaunchArguments(args)
            // Unknown values must not clobber the existing override.
            #expect(MetadataActivationConfig.resolved() == .allEnabled)
        }
    }
    #endif
}
