// InstallIdentity.swift
// Per-install UUID provisioning for the support-safe diagnostics bundle.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Why SwiftData (not Keychain):
//   The codebase has no existing Keychain wrapper, and adding one would be
//   a unilateral persistence-strategy swap (CLAUDE.md "no unilateral
//   swaps"). The bead spec explicitly allows SwiftData as an alternative
//   storage. The trade-off versus Keychain: a SwiftData-backed install ID
//   does NOT survive an app uninstall/reinstall, which is fine — we want a
//   per-install salt (uninstall + reinstall is a new "install" by every
//   meaningful definition).
//
// Schema migration: ``InstallIdentity`` is a new model. It joins the
// shared ``SwiftDataStore.schema`` and the ``PlayheadSchemaV1`` model
// list as a non-destructive additive change (a new entity that the
// lightweight migrator simply creates).
//
// Singleton invariant: this entity is intentionally a single-row table.
// ``InstallIDProvider`` enforces "fetch-or-create" so a second concurrent
// caller cannot insert a duplicate. SwiftData has no first-class
// "singleton table" annotation, so the invariant is enforced by code,
// not schema.

import Foundation
import SwiftData

// MARK: - Model

/// Single-row SwiftData model that holds the per-install UUID consumed
/// by ``EpisodeIdHasher``. Persisting the UUID separately from the
/// `Episode` graph avoids tying its lifetime to any user-visible
/// content; deleting all podcasts and episodes does NOT rotate the
/// install ID.
@Model
final class InstallIdentity {
    var installID: UUID
    var createdAt: Date

    init(installID: UUID = UUID(), createdAt: Date = .now) {
        self.installID = installID
        self.createdAt = createdAt
    }
}

// MARK: - Provider

/// Reads (or provisions on first launch) the per-install UUID. Backed by
/// the SwiftData store so the value survives app launches; lost on
/// uninstall/reinstall by design (uninstall is a new install).
///
/// Thread-safety: SwiftData's ``ModelContext`` is not `Sendable`. Call
/// this provider from the same isolation as the context — typically the
/// `@MainActor` context vended by ``SwiftDataStore.makeContainer()``.
struct InstallIDProvider {

    private let context: ModelContext

    init(context: ModelContext) {
        self.context = context
    }

    /// Returns the persisted install UUID, creating + persisting one on
    /// first call. Throws only if the underlying SwiftData fetch / save
    /// fails — callers should treat any error as a hard failure (the
    /// diagnostics bundle cannot be safely emitted without a salt).
    func installID() throws -> UUID {
        let descriptor = FetchDescriptor<InstallIdentity>()
        let existing = try context.fetch(descriptor)
        if let row = existing.first {
            return row.installID
        }
        let row = InstallIdentity()
        context.insert(row)
        try context.save()
        return row.installID
    }
}
