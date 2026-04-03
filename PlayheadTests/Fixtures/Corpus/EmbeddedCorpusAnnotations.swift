// EmbeddedCorpusAnnotations.swift
// In-memory embedded corpus annotations for testing without requiring
// bundle resources. Contains ~15 realistic sample annotations spanning
// different genres, ad styles, and edge cases.
//
// Audio file references are placeholders. See addRealEpisodes() docs
// in CorpusLoader for how to replace with actual recordings.

import Foundation

// MARK: - Embedded Corpus

/// Provides in-memory JSON data for corpus annotations when bundle resources
/// are not available (unit test targets without resource bundles).
enum EmbeddedCorpusAnnotations {

    static func data(for filename: String) -> Data? {
        switch filename {
        case "manifest":
            return manifestJSON.data(using: .utf8)
        default:
            return annotations[filename]?.data(using: .utf8)
        }
    }

    // MARK: - Manifest

    static let manifestJSON = """
    {
        "corpusVersion": "1.0.0",
        "description": "Labeled test corpus for Playhead ad detection evaluation",
        "lastUpdated": "2026-04-02",
        "episodes": [
            {"annotationFile": "tech-weekly-ep142.json", "tags": ["tech", "midroll", "dynamic-insertion"]},
            {"annotationFile": "tech-weekly-ep143.json", "tags": ["tech", "midroll", "per-show-priors"]},
            {"annotationFile": "true-crime-ep87.json", "tags": ["true-crime", "host-read", "blended"]},
            {"annotationFile": "comedy-hour-ep301.json", "tags": ["comedy", "back-to-back", "multiple-ads"]},
            {"annotationFile": "news-daily-apr01.json", "tags": ["news", "preroll", "short-episode"]},
            {"annotationFile": "history-deep-ep55.json", "tags": ["history", "long-episode", "host-read"]},
            {"annotationFile": "science-pod-ep22.json", "tags": ["science", "produced-segment", "jingle"]},
            {"annotationFile": "interview-show-ep88.json", "tags": ["interview", "blended-host-read", "hard"]},
            {"annotationFile": "sports-recap-ep200.json", "tags": ["sports", "dynamic-insertion", "short-ad"]},
            {"annotationFile": "health-wellness-ep15.json", "tags": ["health", "midroll", "host-read"]},
            {"annotationFile": "business-brief-ep63.json", "tags": ["business", "preroll", "postroll"]},
            {"annotationFile": "storytelling-ep44.json", "tags": ["storytelling", "no-ads", "false-positive-test"]},
            {"annotationFile": "music-talk-ep77.json", "tags": ["music", "very-short-ad", "edge-case"]},
            {"annotationFile": "gaming-pod-ep112.json", "tags": ["gaming", "dynamic-insertion", "variant-test"]},
            {"annotationFile": "parenting-ep31.json", "tags": ["parenting", "blended-host-read", "back-to-back"]}
        ]
    }
    """

    // MARK: - Annotations

    static let annotations: [String: String] = [

        // 1. Tech Weekly Ep 142 - Standard dynamic insertion mid-roll
        "tech-weekly-ep142": """
        {
            "annotationId": "ann-tech-142",
            "audioFileReference": "audio/tech-weekly-ep142.m4a",
            "podcast": {
                "podcastId": "pod-tech-weekly",
                "title": "Tech Weekly",
                "author": "Sarah Chen",
                "genre": "Technology",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-tech-142",
                "title": "The Future of On-Device AI",
                "duration": 3420,
                "publishedAt": "2026-03-28",
                "feedURL": "https://feeds.example.com/techweekly/rss",
                "audioURL": "https://cdn.example.com/techweekly/ep142.m4a"
            },
            "adSegments": [
                {
                    "startTime": 180.0,
                    "endTime": 240.5,
                    "advertiser": "Squarespace",
                    "product": "Website Builder",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Standard DAI with clear music bed transition"
                },
                {
                    "startTime": 1710.0,
                    "endTime": 1775.0,
                    "advertiser": "NordVPN",
                    "product": "VPN Service",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Second mid-roll break with distinct pre-roll jingle"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["tech", "midroll", "dynamic-insertion"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 2. Tech Weekly Ep 143 - Same show for per-show priors testing
        "tech-weekly-ep143": """
        {
            "annotationId": "ann-tech-143",
            "audioFileReference": "audio/tech-weekly-ep143.m4a",
            "podcast": {
                "podcastId": "pod-tech-weekly",
                "title": "Tech Weekly",
                "author": "Sarah Chen",
                "genre": "Technology",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-tech-143",
                "title": "Apple Intelligence Two Years In",
                "duration": 3180,
                "publishedAt": "2026-04-01",
                "feedURL": "https://feeds.example.com/techweekly/rss",
                "audioURL": "https://cdn.example.com/techweekly/ep143.m4a"
            },
            "adSegments": [
                {
                    "startTime": 175.0,
                    "endTime": 235.0,
                    "advertiser": "BetterHelp",
                    "product": "Online Therapy",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Same ad slot position as ep142 (~3min mark)"
                },
                {
                    "startTime": 1590.0,
                    "endTime": 1650.0,
                    "advertiser": "Squarespace",
                    "product": "Website Builder",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Same ad slot position as ep142 (~26.5min mark)"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["tech", "midroll", "per-show-priors"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 3. True Crime - Host-read with blended delivery
        "true-crime-ep87": """
        {
            "annotationId": "ann-crime-87",
            "audioFileReference": "audio/true-crime-ep87.m4a",
            "podcast": {
                "podcastId": "pod-true-crime",
                "title": "Cold Case Files Revisited",
                "author": "Marcus Webb",
                "genre": "True Crime",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-crime-87",
                "title": "The Vanishing at Pine Lake",
                "duration": 4200,
                "publishedAt": "2026-03-25",
                "feedURL": "https://feeds.example.com/coldcase/rss",
                "audioURL": "https://cdn.example.com/coldcase/ep87.m4a"
            },
            "adSegments": [
                {
                    "startTime": 300.0,
                    "endTime": 390.0,
                    "advertiser": "HelloFresh",
                    "product": "Meal Kit Delivery",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Host transitions from case discussion to ad with 'speaking of things that save time...'"
                },
                {
                    "startTime": 2100.0,
                    "endTime": 2175.0,
                    "advertiser": "Athletic Greens",
                    "product": "AG1 Supplement",
                    "adType": "midRoll",
                    "deliveryStyle": "blendedHostRead",
                    "difficulty": "hard",
                    "notes": "Host weaves AG1 mention into discussion about the detective's daily routine"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["true-crime", "host-read", "blended"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 4. Comedy Hour - Back-to-back ads
        "comedy-hour-ep301": """
        {
            "annotationId": "ann-comedy-301",
            "audioFileReference": "audio/comedy-hour-ep301.m4a",
            "podcast": {
                "podcastId": "pod-comedy-hour",
                "title": "The Comedy Hour",
                "author": "Jake & Lisa",
                "genre": "Comedy",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-comedy-301",
                "title": "When Autocorrect Attacks",
                "duration": 3600,
                "publishedAt": "2026-03-30",
                "feedURL": "https://feeds.example.com/comedyhour/rss",
                "audioURL": "https://cdn.example.com/comedyhour/ep301.m4a"
            },
            "adSegments": [
                {
                    "startTime": 240.0,
                    "endTime": 300.0,
                    "advertiser": "ExpressVPN",
                    "product": "VPN Service",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "First of three back-to-back ads"
                },
                {
                    "startTime": 300.0,
                    "endTime": 360.0,
                    "advertiser": "Calm",
                    "product": "Meditation App",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Second back-to-back, no gap"
                },
                {
                    "startTime": 360.0,
                    "endTime": 420.0,
                    "advertiser": "ZipRecruiter",
                    "product": "Job Platform",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Third back-to-back, no gap"
                },
                {
                    "startTime": 1800.0,
                    "endTime": 1870.0,
                    "advertiser": "Manscaped",
                    "product": "Grooming Products",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Host incorporates comedy into the ad read"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["comedy", "back-to-back", "multiple-ads"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 5. News Daily - Short episode with pre-roll
        "news-daily-apr01": """
        {
            "annotationId": "ann-news-apr01",
            "audioFileReference": "audio/news-daily-apr01.m4a",
            "podcast": {
                "podcastId": "pod-news-daily",
                "title": "The Daily Brief",
                "author": "NPR",
                "genre": "News",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-news-apr01",
                "title": "April 1 Headlines",
                "duration": 900,
                "publishedAt": "2026-04-01",
                "feedURL": "https://feeds.example.com/dailybrief/rss",
                "audioURL": "https://cdn.example.com/dailybrief/apr01.m4a"
            },
            "adSegments": [
                {
                    "startTime": 0.0,
                    "endTime": 30.0,
                    "advertiser": "Indeed",
                    "product": "Job Search",
                    "adType": "preRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Pre-roll before episode intro"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["news", "preroll", "short-episode"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 6. History Deep Dive - Long episode with host-read
        "history-deep-ep55": """
        {
            "annotationId": "ann-history-55",
            "audioFileReference": "audio/history-deep-ep55.m4a",
            "podcast": {
                "podcastId": "pod-history-deep",
                "title": "History Deep Dive",
                "author": "Dr. Emily Park",
                "genre": "History",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-history-55",
                "title": "The Fall of Constantinople",
                "duration": 7200,
                "publishedAt": "2026-03-20",
                "feedURL": "https://feeds.example.com/historydeep/rss",
                "audioURL": "https://cdn.example.com/historydeep/ep55.m4a"
            },
            "adSegments": [
                {
                    "startTime": 600.0,
                    "endTime": 690.0,
                    "advertiser": "Audible",
                    "product": "Audiobook Service",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Host recommends a specific audiobook"
                },
                {
                    "startTime": 3600.0,
                    "endTime": 3680.0,
                    "advertiser": "Masterclass",
                    "product": "Online Learning",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Mid-episode break at the halfway point"
                },
                {
                    "startTime": 5400.0,
                    "endTime": 5475.0,
                    "advertiser": "SimpliSafe",
                    "product": "Home Security",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "easy",
                    "notes": "Clear transition with 'let me take a moment to tell you about...'"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["history", "long-episode", "host-read"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 7. Science Pod - Produced segment with jingle
        "science-pod-ep22": """
        {
            "annotationId": "ann-science-22",
            "audioFileReference": "audio/science-pod-ep22.m4a",
            "podcast": {
                "podcastId": "pod-science",
                "title": "Quantum Questions",
                "author": "Dr. Raj Patel",
                "genre": "Science",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-science-22",
                "title": "Quantum Computing Hits 1000 Qubits",
                "duration": 2700,
                "publishedAt": "2026-03-22",
                "feedURL": "https://feeds.example.com/quantum/rss",
                "audioURL": "https://cdn.example.com/quantum/ep22.m4a"
            },
            "adSegments": [
                {
                    "startTime": 420.0,
                    "endTime": 480.0,
                    "advertiser": "Brilliant",
                    "product": "Online Learning",
                    "adType": "midRoll",
                    "deliveryStyle": "producedSegment",
                    "difficulty": "easy",
                    "notes": "Pre-produced segment with distinct jingle in/out"
                },
                {
                    "startTime": 1350.0,
                    "endTime": 1420.0,
                    "advertiser": "Wren",
                    "product": "Carbon Offsets",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Host-read with personal anecdote about carbon footprint"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["science", "produced-segment", "jingle"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 8. Interview Show - Blended host-read (hard detection)
        "interview-show-ep88": """
        {
            "annotationId": "ann-interview-88",
            "audioFileReference": "audio/interview-show-ep88.m4a",
            "podcast": {
                "podcastId": "pod-interview",
                "title": "Deep Conversations",
                "author": "Mia Torres",
                "genre": "Society & Culture",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-interview-88",
                "title": "Building a Billion-Dollar Startup with Grace Kim",
                "duration": 4800,
                "publishedAt": "2026-03-27",
                "feedURL": "https://feeds.example.com/deepconv/rss",
                "audioURL": "https://cdn.example.com/deepconv/ep88.m4a"
            },
            "adSegments": [
                {
                    "startTime": 240.0,
                    "endTime": 340.0,
                    "advertiser": "LinkedIn",
                    "product": "LinkedIn Premium",
                    "adType": "midRoll",
                    "deliveryStyle": "blendedHostRead",
                    "difficulty": "hard",
                    "notes": "Host naturally transitions from discussing Grace's career to LinkedIn's role in networking. Very blended."
                },
                {
                    "startTime": 2400.0,
                    "endTime": 2470.0,
                    "advertiser": "Notion",
                    "product": "Productivity Software",
                    "adType": "midRoll",
                    "deliveryStyle": "blendedHostRead",
                    "difficulty": "hard",
                    "notes": "Discussion of Grace's workflow leads into Notion mention. Guest even comments on it."
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["interview", "blended-host-read", "hard"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 9. Sports Recap - Dynamic insertion, very short ad
        "sports-recap-ep200": """
        {
            "annotationId": "ann-sports-200",
            "audioFileReference": "audio/sports-recap-ep200.m4a",
            "podcast": {
                "podcastId": "pod-sports-recap",
                "title": "The Sports Desk",
                "author": "Chris & Pat",
                "genre": "Sports",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-sports-200",
                "title": "March Madness Final Four Preview",
                "duration": 2400,
                "publishedAt": "2026-03-29",
                "feedURL": "https://feeds.example.com/sportsdesk/rss",
                "audioURL": "https://cdn.example.com/sportsdesk/ep200.m4a"
            },
            "adSegments": [
                {
                    "startTime": 10.0,
                    "endTime": 25.0,
                    "advertiser": "DraftKings",
                    "product": "Sports Betting",
                    "adType": "preRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Very short 15-second pre-roll bumper"
                },
                {
                    "startTime": 1200.0,
                    "endTime": 1265.0,
                    "advertiser": "FanDuel",
                    "product": "Fantasy Sports",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Standard mid-roll with music bed"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["sports", "dynamic-insertion", "short-ad"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 10. Health & Wellness
        "health-wellness-ep15": """
        {
            "annotationId": "ann-health-15",
            "audioFileReference": "audio/health-wellness-ep15.m4a",
            "podcast": {
                "podcastId": "pod-health",
                "title": "Mindful Living",
                "author": "Dr. Amy Liu",
                "genre": "Health & Fitness",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-health-15",
                "title": "Sleep Science: What Actually Works",
                "duration": 3000,
                "publishedAt": "2026-03-26",
                "feedURL": "https://feeds.example.com/mindful/rss",
                "audioURL": "https://cdn.example.com/mindful/ep15.m4a"
            },
            "adSegments": [
                {
                    "startTime": 450.0,
                    "endTime": 530.0,
                    "advertiser": "Eight Sleep",
                    "product": "Smart Mattress",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Host discusses sleep quality then transitions to Eight Sleep endorsement"
                },
                {
                    "startTime": 1500.0,
                    "endTime": 1570.0,
                    "advertiser": "Helix Sleep",
                    "product": "Mattress",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Second mattress sponsor, different brand in same sleep episode"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["health", "midroll", "host-read"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 11. Business Brief - Pre-roll and post-roll
        "business-brief-ep63": """
        {
            "annotationId": "ann-business-63",
            "audioFileReference": "audio/business-brief-ep63.m4a",
            "podcast": {
                "podcastId": "pod-business",
                "title": "The Business Brief",
                "author": "David Park",
                "genre": "Business",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-business-63",
                "title": "Why Remote Work Won",
                "duration": 1800,
                "publishedAt": "2026-03-31",
                "feedURL": "https://feeds.example.com/bizbrief/rss",
                "audioURL": "https://cdn.example.com/bizbrief/ep63.m4a"
            },
            "adSegments": [
                {
                    "startTime": 0.0,
                    "endTime": 35.0,
                    "advertiser": "Shopify",
                    "product": "E-Commerce Platform",
                    "adType": "preRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Pre-roll before intro music"
                },
                {
                    "startTime": 1750.0,
                    "endTime": 1800.0,
                    "advertiser": "Monday.com",
                    "product": "Project Management",
                    "adType": "postRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Post-roll after sign-off"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["business", "preroll", "postroll"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 12. Storytelling - No ads (false positive test)
        "storytelling-ep44": """
        {
            "annotationId": "ann-story-44",
            "audioFileReference": "audio/storytelling-ep44.m4a",
            "podcast": {
                "podcastId": "pod-storytelling",
                "title": "Fireside Tales",
                "author": "Robin Hayes",
                "genre": "Fiction",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-story-44",
                "title": "The Lighthouse Keeper's Daughter",
                "duration": 2400,
                "publishedAt": "2026-03-24",
                "feedURL": "https://feeds.example.com/fireside/rss",
                "audioURL": "https://cdn.example.com/fireside/ep44.m4a"
            },
            "adSegments": [],
            "isNoAdEpisode": true,
            "tags": ["storytelling", "no-ads", "false-positive-test"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 13. Music Talk - Very short ad edge case
        "music-talk-ep77": """
        {
            "annotationId": "ann-music-77",
            "audioFileReference": "audio/music-talk-ep77.m4a",
            "podcast": {
                "podcastId": "pod-music-talk",
                "title": "Vinyl & Frequencies",
                "author": "DJ Nova",
                "genre": "Music",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-music-77",
                "title": "The Resurgence of Cassette Culture",
                "duration": 3300,
                "publishedAt": "2026-03-23",
                "feedURL": "https://feeds.example.com/vinyl/rss",
                "audioURL": "https://cdn.example.com/vinyl/ep77.m4a"
            },
            "adSegments": [
                {
                    "startTime": 330.0,
                    "endTime": 342.0,
                    "advertiser": "Bandcamp",
                    "product": "Music Platform",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "medium",
                    "notes": "Very short 12-second bumper ad. Below typical minimum-span threshold."
                },
                {
                    "startTime": 1650.0,
                    "endTime": 1720.0,
                    "advertiser": "Fender",
                    "product": "Guitars",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Normal-length host read about Fender's new line"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["music", "very-short-ad", "edge-case"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 14. Gaming Pod - Dynamic insertion with variants
        "gaming-pod-ep112": """
        {
            "annotationId": "ann-gaming-112",
            "audioFileReference": "audio/gaming-pod-ep112.m4a",
            "podcast": {
                "podcastId": "pod-gaming",
                "title": "Level Up",
                "author": "Alex & Jordan",
                "genre": "Leisure Games",
                "usesDynamicAdInsertion": true
            },
            "episode": {
                "episodeId": "ep-gaming-112",
                "title": "GTA 7 First Impressions",
                "duration": 4500,
                "publishedAt": "2026-03-29",
                "feedURL": "https://feeds.example.com/levelup/rss",
                "audioURL": "https://cdn.example.com/levelup/ep112.m4a"
            },
            "adSegments": [
                {
                    "startTime": 300.0,
                    "endTime": 365.0,
                    "advertiser": "Razer",
                    "product": "Gaming Peripherals",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Standard DAI slot. Different ad may appear in variant listens."
                },
                {
                    "startTime": 2250.0,
                    "endTime": 2320.0,
                    "advertiser": "CuriosityStream",
                    "product": "Documentary Streaming",
                    "adType": "midRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Second DAI slot"
                },
                {
                    "startTime": 4400.0,
                    "endTime": 4460.0,
                    "advertiser": "Ridge Wallet",
                    "product": "Slim Wallet",
                    "adType": "postRoll",
                    "deliveryStyle": "dynamicInsertion",
                    "difficulty": "easy",
                    "notes": "Post-roll"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["gaming", "dynamic-insertion", "variant-test"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,

        // 15. Parenting - Blended host-read + back-to-back
        "parenting-ep31": """
        {
            "annotationId": "ann-parenting-31",
            "audioFileReference": "audio/parenting-ep31.m4a",
            "podcast": {
                "podcastId": "pod-parenting",
                "title": "Good Enough Parenting",
                "author": "Sam & Alex Rivera",
                "genre": "Kids & Family",
                "usesDynamicAdInsertion": false
            },
            "episode": {
                "episodeId": "ep-parenting-31",
                "title": "Screen Time: Finding the Balance",
                "duration": 3900,
                "publishedAt": "2026-03-28",
                "feedURL": "https://feeds.example.com/goodenough/rss",
                "audioURL": "https://cdn.example.com/goodenough/ep31.m4a"
            },
            "adSegments": [
                {
                    "startTime": 480.0,
                    "endTime": 560.0,
                    "advertiser": "KiwiCo",
                    "product": "Kids Activity Kits",
                    "adType": "midRoll",
                    "deliveryStyle": "blendedHostRead",
                    "difficulty": "hard",
                    "notes": "Host discusses screen time alternatives and naturally brings up KiwiCo kits their kids use"
                },
                {
                    "startTime": 560.0,
                    "endTime": 625.0,
                    "advertiser": "Lovevery",
                    "product": "Play Kits",
                    "adType": "midRoll",
                    "deliveryStyle": "blendedHostRead",
                    "difficulty": "hard",
                    "notes": "Immediately follows KiwiCo -- back-to-back blended reads. Very hard boundary."
                },
                {
                    "startTime": 1950.0,
                    "endTime": 2020.0,
                    "advertiser": "Greenlight",
                    "product": "Kids Debit Card",
                    "adType": "midRoll",
                    "deliveryStyle": "hostRead",
                    "difficulty": "medium",
                    "notes": "Standard host-read mid-roll about teaching kids financial literacy"
                }
            ],
            "isNoAdEpisode": false,
            "tags": ["parenting", "blended-host-read", "back-to-back"],
            "schemaVersion": 1,
            "annotatedBy": "corpus-generator",
            "lastUpdated": "2026-04-02"
        }
        """,
    ]
}
