// InstallIdentityTests.swift
// Verifies the per-install UUID provisioned by ``InstallIdentity`` and
// surfaced through ``InstallIDProvider``.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Spec contracts under test:
//   * `InstallIDProvider` returns the SAME UUID across two reads on the
//     same install (per-install stable).
//   * Two separate installs each provision their own distinct UUID.
//   * The UUID is stored in SwiftData (the existing persistence
//     wrapper — Keychain wrapper is intentionally absent in this codebase).

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("InstallIdentity / InstallIDProvider — per-install stable UUID (playhead-ghon)")
@MainActor
struct InstallIdentityTests {

    // MARK: - Helpers

    private func makeInMemoryContext() throws -> ModelContext {
        let schema = Schema([InstallIdentity.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        return ModelContext(container)
    }

    // MARK: - Stability across reads

    @Test("two reads on the same install return the same UUID")
    func stableAcrossReads() throws {
        let ctx = try makeInMemoryContext()
        let provider = InstallIDProvider(context: ctx)
        let first = try provider.installID()
        let second = try provider.installID()
        #expect(first == second)
    }

    // MARK: - Distinct installs

    @Test("two separate installs (separate stores) get distinct UUIDs")
    func distinctInstalls() throws {
        let ctx1 = try makeInMemoryContext()
        let ctx2 = try makeInMemoryContext()
        let id1 = try InstallIDProvider(context: ctx1).installID()
        let id2 = try InstallIDProvider(context: ctx2).installID()
        #expect(id1 != id2)
    }

    // MARK: - Persistence

    @Test("UUID survives a fresh provider instance against the same context")
    func surviveProviderRecreation() throws {
        let ctx = try makeInMemoryContext()
        let id1 = try InstallIDProvider(context: ctx).installID()
        // Drop the provider; a fresh one should observe the same persisted row.
        let id2 = try InstallIDProvider(context: ctx).installID()
        #expect(id1 == id2)
    }

    // MARK: - Singleton row invariant

    @Test("provisioning never creates a second row")
    func singletonRow() throws {
        let ctx = try makeInMemoryContext()
        _ = try InstallIDProvider(context: ctx).installID()
        _ = try InstallIDProvider(context: ctx).installID()
        let descriptor = FetchDescriptor<InstallIdentity>()
        let rows = try ctx.fetch(descriptor)
        #expect(rows.count == 1)
    }
}
