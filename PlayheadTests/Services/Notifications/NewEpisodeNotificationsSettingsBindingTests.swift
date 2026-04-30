// NewEpisodeNotificationsSettingsBindingTests.swift
// playhead-snp — pin the side-effect contract for toggling app-wide
// new-episode notifications: when the master flag flips OFF, any
// pending (not-yet-delivered) new-episode notifications must be
// canceled so they don't fire after the user opted out.

import Foundation
import Testing

@testable import Playhead

@Suite("NewEpisodeNotificationsSettingsBinding — toggle off cancels pending (playhead-snp)")
@MainActor
struct NewEpisodeNotificationsSettingsBindingTests {

    private actor RecordingCancellation: PendingNewEpisodeNotificationCancelling {
        private(set) var calls = 0
        func cancelAllPendingNewEpisodeNotifications() async {
            calls += 1
        }
    }

    @Test("Toggling OFF triggers the cancel-pending hook")
    func togglingOffCancelsPending() async {
        let cancel = RecordingCancellation()
        var stored = true
        let binding = NewEpisodeNotificationsSettingsBinding(
            current: { stored },
            update: { stored = $0 },
            cancellation: cancel
        )

        await binding.set(false)

        #expect(stored == false)
        #expect(await cancel.calls == 1)
    }

    @Test("Toggling ON does NOT trigger the cancel-pending hook")
    func togglingOnDoesNotCancel() async {
        let cancel = RecordingCancellation()
        var stored = false
        let binding = NewEpisodeNotificationsSettingsBinding(
            current: { stored },
            update: { stored = $0 },
            cancellation: cancel
        )

        await binding.set(true)

        #expect(stored == true)
        #expect(await cancel.calls == 0)
    }

    @Test("Setting the same value is a no-op (no spurious cancel)")
    func sameValueNoop() async {
        let cancel = RecordingCancellation()
        var stored = false
        let binding = NewEpisodeNotificationsSettingsBinding(
            current: { stored },
            update: { stored = $0 },
            cancellation: cancel
        )

        await binding.set(false)

        #expect(stored == false)
        #expect(await cancel.calls == 0)
    }
}
