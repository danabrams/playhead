// ConanFanhausenRevisitedFixture.swift
// Real podcast transcript + ground truth for benchmarking Phase 2 components
// against actual ad detection scenarios. Captured via DEBUG export feature on
// 2026-04-06 from the Playhead iOS app.
//
// Episode: "Fanhausen Revisited" from "Conan O'Brien Needs A Friend"
// Duration: ~16:30
// Asset ID (original capture): 2CD84FDA-975F-4580-8D19-02CEAEF08757
//
// Ground truth was confirmed by a human listener walking through the episode.

import Foundation
@testable import Playhead

// MARK: - Ground truth types

enum GroundTruthAdType: String, Sendable {
    /// Third-party paid sponsor (CVS, SiriusXM, etc.)
    case sponsor
    /// Podcast cross-promo (e.g. Kelly Ripa "Let's Talk Off Camera")
    case crossPromo
    /// Host-read sponsor integration baked into another segment (e.g. credits)
    case integration
}

struct GroundTruthAd: Sendable {
    let id: String
    let startTime: Double
    let endTime: Double
    let type: GroundTruthAdType
    /// Confidence that a typical user wants this skipped: 0.0...1.0.
    /// 1.0 = definitely an ad, 0.5 = edge case, 0.0 = leave alone.
    let skipConfidence: Double
    let advertiser: String
    let description: String
    /// Phrases the detector should pick up (for tracking signal coverage over time).
    let expectedSignals: [String]
    /// Phrases that would help but are not currently in the pattern list.
    let missedSignals: [String]

    var duration: Double { endTime - startTime }
}

struct NonAdSignal: Sendable {
    let startTime: Double
    let endTime: Double
    let description: String
    let expectedPattern: String
    let reason: String
}

// MARK: - Fixture

enum ConanFanhausenRevisitedFixture {

    static let assetId = "conan-fanhausen-revisited"
    static let episodeId = "conan-fanhausen-revisited"
    static let episodeTitle = "Fanhausen Revisited"
    static let podcastTitle = "Conan O'Brien Needs A Friend"
    static let duration: Double = 990  // ~16:30

    /// Ground-truth ad segments confirmed by a human listener.
    static let groundTruthAds: [GroundTruthAd] = [
        GroundTruthAd(
            id: "cvs-preroll",
            startTime: 0,
            endTime: 26,
            type: .sponsor,
            skipConfidence: 1.0,
            advertiser: "CVS",
            description: "CVS pharmacy pre-roll - vaccines",
            expectedSignals: [
                "cvs.com at 0:21-0:26 (URL pattern)",
            ],
            missedSignals: [
                "CVS mentioned at 0:07 but ASR heard 'CBS pharmacists'",
                "'schedule yours today' not in CTA pattern list",
                "no disclosure phrase",
            ]
        ),
        GroundTruthAd(
            id: "kelly-ripa-1",
            startTime: 30,
            endTime: 56,
            type: .crossPromo,
            skipConfidence: 0.8,
            advertiser: "Kelly Ripa - Let's Talk Off Camera",
            description: "Podcast cross-promo, conversational",
            expectedSignals: [
                // None currently. This is the architectural gap Phase 3 (FM scanner) closes.
            ],
            missedSignals: [
                "'listen to ... wherever you get your podcasts' is no URL, no promo, no disclosure",
                "'celebrating 3 years of my podcast' is a podcast cross-promo cue",
            ]
        ),
        GroundTruthAd(
            id: "siriusxm-credits",
            startTime: 952,
            endTime: 959,
            type: .integration,
            skipConfidence: 0.5,
            advertiser: "SiriusXM",
            description: "SiriusXM integration baked into outro credits",
            expectedSignals: [
                "siriusxm.com at 15:52-15:59 (URL pattern)",
                "'sign up at' near URL (CTA-adjacent)",
            ],
            missedSignals: [
                "'three free months' is a commercial offer but not in pattern list",
            ]
        ),
        GroundTruthAd(
            id: "kelly-ripa-2",
            startTime: 971,
            endTime: 989,
            type: .crossPromo,
            skipConfidence: 0.8,
            advertiser: "Kelly Ripa - Let's Talk Off Camera (repeat)",
            description: "Same cross-promo as kelly-ripa-1, repeated at end of episode",
            expectedSignals: [],
            missedSignals: [
                "Same as kelly-ripa-1: zero pattern matches",
            ]
        ),
    ]

    /// Non-ad content that currently triggers signals but should NOT be flagged.
    static let knownFalsePositives: [NonAdSignal] = [
        NonAdSignal(
            startTime: 197,
            endTime: 205,
            description: "teamcoco.com slash call Conan - first-party call-in instructions",
            expectedPattern: "teamcoco.com (URL pattern)",
            reason: "First-party domain, show structure not sponsor ad. Phase 8 (first-party domain tracking) would suppress this."
        ),
    ]

    // MARK: - Raw transcript

    /// Raw transcript in the format: "[M:SS-M:SS] text".
    /// 312 lines captured from Playhead's DEBUG export feature.
    /// The original capture had a "(fast) " pass tag after each timestamp; the
    /// parser tolerates lines with or without it.
    static let rawTranscript = """
    [0:00-0:07] Getting your vaccines matters, but with so much confusing information out there, it can be hard to know for sure.
    [0:07-0:17] Your local CBS pharmacists are here to help, with the answers to your questions, and important vaccines like shingles, RSV, and pneumococcal pneumonia if eligible.
    [0:17-0:21] So you can get the protection you need and peace of mind too.
    [0:21-0:26] Schedule yours today at cvs.com or on the CVS Health app.
    [0:30-0:35] Hey everyone, it's Kelly Ripper, and we're celebrating 3 years of my podcast.
    [0:35-0:37] Let's talk off camera.
    [0:37-0:52] No hair, no makeup, just 3 great years of the most honest conversations, real stories, and unfiltered talk, and we're joined every week by celebrity guests like Nicky Glaser, Kate Hudson, Oprah, and more, 3 years in, and we're not done yet.
    [0:52-0:56] Listen to let's talk off camera wherever you get your podcasts.
    [1:00-1:13] Well, we have come up on a very nice anniversary, hard to believe, but 5 years ago this month, we did our very 1st fan episode.
    [1:13-1:17] And, um, it's been 5 years. And I really love these segments.
    [1:17-1:21] And the very 1st one featured a fan of mine named Dan Hausen.
    [1:21-1:30] Now, Dan Hausen is a wrestler, and he explained to me when we did this very 1st fan episode that he had loosely based his character, his wrestler.
    [1:30-1:34] character on me if I was an interdimensional demon.
    [1:34-1:37] Which seems redundant to me.
    [1:37-1:40] And Dan Hauser and I had a great conversation.
    [1:40-1:44] We talked about comedy, performance, the love of entertaining people.
    [1:44-1:56] And what struck me, and still, I remember this to this day, Dan Hausen told me that he had been grinding away for the past 8 years driving 12 hours every weekend, just be able to get up in front of people and wrestle.
    [1:56-2:00] And this is a guy who just applied an incredible work.
    [2:00-2:01] ethic to his passion.
    [2:02-2:04] And I was so impressed with this fellow.
    [2:04-2:14] Well, now it's 5 years later, and I am thrilled to report that Danhausen has recently made his WWE debut to rave reviews.
    [2:14-2:23] And this is proof again that if you can marry hard work to your passion, you can go places.
    [2:23-2:30] So on behalf of myself and all of us here at Team Coco, massive congratulations and mad respect.
    [2:30-2:34] to Danhausen, and I'm so proud of you, happy for you.
    [2:34-2:39] And, um, I just should mention, because your character is based on me, I'm getting 20%.
    [2:39-2:41] Oh, are you getting 20%?
    [2:41-2:46] Well, I will, once I unleash my interdimensional demons.
    [2:46-2:48] Rick Rosen.
    [2:48-2:56] From William Morrison Endeavor, and the blackest, blackest heart of all, Gavin Palone.
    [2:56-3:00] I mean, when they're done with Dan Hausen,
    [3:00-3:05] He's just gonna be some flesh clinging to a battered vertebrae.
    [3:05-3:10] Uh, anyway, um, uh, I'm just, I'm just strolling for him.
    [3:10-3:11] So here, this is unusual for us.
    [3:11-3:17] revisit my chat from 5 years ago with the one, the only Danhausen.
    [3:17-3:17] Enjoy.
    [3:17-3:21] Conan O'Brien needs a fan.
    [3:21-3:22] Want to talk to Conan?
    [3:22-3:25] Visit teamcoco.com slash call Conan.
    [3:26-3:27] Okay, let's get started.
    [3:27-3:30] Hey, everybody, Conan O'Brien.
    [3:30-3:35] and we're going to try something a little different in this short time that I've been doing.
    [3:35-3:37] Conan O'Brien needs a friend.
    [3:37-3:39] I've just been delighted.
    [3:39-3:43] I'm having an absolute blast and it's working.
    [3:43-3:52] I'm actually making some nice bonds and friendships with a lot of different people, but what occurred to me is all these people have one thing in common.
    [3:52-3:53] They are celebrities.
    [3:53-4:00] And I thought it might be nice to try making friends with average.
    [4:00-4:04] folk, people out there in the world, civilians, not celebrities.
    [4:04-4:08] Just talk to the people who make this great country.
    [4:08-4:11] we call the United States America, or even people from other countries.
    [4:11-4:18] It doesn't matter. Let's just talk to some regular folk, and then hope, hope desperately, that they become celebrities.
    [4:18-4:22] Oh my God. That's the concept.
    [4:22-4:22] What do you guys think?
    [4:23-4:26] Yeah, why is why is that horrible?
    [4:26-4:29] It's very important to me that eventually they become celebrities.
    [4:30-4:34] So you don't have time for anybody that would live their whole life as a regular Joe.
    [4:34-4:35] As a folk.
    [4:35-4:37] Who would do that?
    [4:37-4:40] What kind of monster would choose that life?
    [4:40-4:48] No, seriously, I really do, I do want to, and especially, I have to say, a lot of this comes out of this last year.
    [4:48-4:54] Let's get outside this bubble, this celebrity bubble that we're trapped in.
    [4:54-4:55] I'm not trapped in a celebrity.
    [4:55-4:56] No, I'm not even a child.
    [4:57-4:57] God, no.
    [4:57-4:58] I didn't mean either of you.
    [4:58-4:59] God, no.
    [4:59-4:30] No, no, no, no.
    [5:00-5:01] No, please.
    [5:01-5:03] Oh, how embarrassing.
    [5:03-5:05] I'm covering my mouth.
    [5:05-5:05] I'm laughing.
    [5:05-5:09] Because, I mean, we don't even do a podcast with a celebrity.
    [5:09-5:10] So how would we know?
    [5:10-5:14] Oh, snap, snappity, dappity, out.
    [5:14-5:15] She wowchy.
    [5:15-5:16] I'm looking it up.
    [5:16-5:18] I'm looking it up.
    [5:18-5:19] I am looking it up.
    [5:19-5:21] Yes, I am a celebrity.
    [5:21-5:22] just looked it up.
    [5:22-5:26] You Googled it? Yeah, I am a B-lister, but I am a celebrity.
    [5:27-5:29] So, uh, okay, yeah.
    [5:30-5:33] I am a solid B.
    [5:33-5:36] I'm a solid B list celebrity, and I'm proud of it.
    [5:36-5:41] And if Loveboat were still on the air, I could I could potentially be a guest.
    [5:41-5:46] Not the 1st guest, but like the 3rd guest, who's the comic relief guest, who stowed away.
    [5:46-5:48] I would kill to see you on love boat.
    [5:48-5:55] Yes, and but anyway, this is this is something I want to do and I'm really looking forward to it.
    [5:56-5:57] And uh, I don't know.
    [5:57-5:59] We're just gonna give it a try and see how it goes.
    [5:59-6:00] Yeah, this is.
    [6:00-6:04] Conan O'Brien needs a fan and it'll be out weekly in addition to the regular episodes.
    [6:04-6:06] So just an extra special treat.
    [6:06-6:08] And we might as well get to our 1st guest.
    [6:09-6:09] Are you guys ready?
    [6:09-6:11] I am very ready.
    [6:11-6:15] Conan, please meet Donovan, who is a minor league professional wrestler.
    [6:15-6:16] Wow.
    [6:16-6:18] Donovan, very nice to talk to you.
    [6:18-6:20] Where are you coming from, Donovan?
    [6:20-6:20] Where are you?
    [6:20-6:23] I am in Michigan right now from Montreal.
    [6:23-6:25] Do you consider yourself a Canadian?
    [6:25-6:26] No, no, I'm from Michigan.
    [6:26-6:27] Oh, you're from Michigan?
    [6:27-6:29] Yeah, sorry, I probably said that wrong.
    [6:30-6:33] Life is from Montreal. Wait, yeah, I'm confused already.
    [6:33-6:35] You're from Michigan.
    [6:35-6:37] You've married someone who's from Montreal.
    [6:37-6:38] Yes, exactly.
    [6:38-6:39] Okay.
    [6:39-6:42] So I'm in the process of getting my permanent residency there.
    [6:42-6:44] Oh, okay, you're going to move to Montreal.
    [6:44-6:45] I'm going, yes.
    [6:45-6:46] Okay.
    [6:46-6:47] Well, that's all the time we have.
    [6:47-6:49] Thank you so much.
    [6:49-6:55] So, uh, Donovan, you are a professional wrestler?
    [6:55-6:55] that right?
    [6:55-6:55] Yes.
    [6:56-7:00] Okay, now, help me because I know of a...
    [7:00-7:13] type of professional wrestler that has a character, and I don't know, are you a professional wrestler who's really wrestling and using wrestling moves, and it's not that fun to watch, or are you a wrestler who's also kind of a performer and has a character?
    [7:13-7:15] I am a character.
    [7:15-7:17] Actually, I have a picture if you want to see it.
    [7:17-7:19] It's, uh, that's me.
    [7:19-7:20] Okay, okay.
    [7:20-7:22] Well, we are a podcast, so I'm going to describe it.
    [7:22-7:24] You're sort of demonic looking.
    [7:24-7:29] You just showed me a picture of what looked like a very fierce, evil, demonic.
    [7:30-7:35] Yes, so I go by the moniker very nice, very evil, because nobody likes somebody who's too evil.
    [7:35-7:39] So I introduced the nice part of it, so then they buy into it and I can trick them.
    [7:39-7:41] Okay, very nice, very evil.
    [7:41-7:45] Often I get described if somebody, a demon possessed you, actually.
    [7:45-7:48] Oh, a demon possessed me.
    [7:48-7:50] If Conan O'Brien was possessed by a demon.
    [7:50-7:54] That's what it gets described as, because I'm heavily influenced by you rather than other wrestlers.
    [7:58-7:59] You mean of the wrestlers.
    [7:59-8:00] I'm the one.
    [8:00-8:02] that's influenced you the most. Yes, yes, of course.
    [8:02-8:03] That's fantastic
    [8:16-8:25] Describe, then, a demonic Conan O'Brien, as a wrestling character, what are you using some of my moves?
    [8:25-8:26] Is it my attitude?
    [8:26-8:29] Does your character have, you know, sort of little beady eyes?
    [8:30-8:32] thin lips and sharp cheekbones.
    [8:32-8:39] Yes, yeah, I don't have the height, but I have, I utilize, so I pour teeth in my opponent's mouths.
    [8:39-8:44] Uh, to disorient them. What in their mouth? Human teeth.
    [8:44-8:46] You pour human teeth into the mouth.
    [8:46-8:50] I love how that's people see that and go, oh, that is so Conan.
    [8:50-8:51] No, no, no.
    [8:51-8:56] It's just, I think it's the presentation because I influ- I take a lot of like Simpsons references.
    [9:00-9:04] and I pull it all together because these are the things that I like.
    [9:04-9:07] So I included it into the character because wrestling should be fun.
    [9:07-9:09] Yes, wrestling's saying it shouldn't be work.
    [9:09-9:09] Yeah.
    [9:10-9:11] No, no.
    [9:11-9:13] So, um, okay.
    [9:13-9:20] One of your standard moves is to pour loose teeth into the mouth of your opponent to confuse and disorient them.
    [9:20-9:23] What are some of your other moments? Kick them right in the mouth.
    [9:23-9:26] I have the go to sleep, which I call the good nighthausen.
    [9:27-9:28] Um, I had housing.
    [9:28-9:30] My wrestling name is...
    [9:30-9:33] Danhausen, and I had Hausen to everything to make it all about me.
    [9:33-9:35] That's very Cody.
    [9:35-9:37] Okay, that's very nice, Matt.
    [9:37-9:37] Yeah.
    [9:37-9:38] I love that.
    [9:38-9:41] You just add a house into things, so it's so good night housing is like a good night move.
    [9:41-9:45] Yes, and I pop them up off my shoulders and I need them in the face.
    [9:45-9:46] That's my finishing.
    [9:46-9:48] Well, you need them in the face house.
    [9:48-9:49] The face?
    [9:49-9:49] yes exactly.
    [9:49-9:50] Sorry.
    [9:50-9:53] Like, if I were talking to you, I'd call you Conenhausen.
    [9:53-9:55] I had housing to the end of it.
    [9:55-9:56] Uh-huh, uh-huh.
    [9:56-9:57] This is fantastic.
    [9:57-9:59] I'm delighted by you.
    [10:00-10:08] I'm delighted by this foolishness and that you've like me dedicated your life to absolute idiocy.
    [10:08-10:10] This is fantastic.
    [10:10-10:12] Now, are you a good wrestler?
    [10:12-10:13] Are you a good athlete?
    [10:13-10:14] Yeah, but that doesn't matter.
    [10:14-10:16] No one cared when I was just a good wrestler.
    [10:17-10:27] They cared once I switched and put on makeup and started acting goofy and doing Simpsons references in the middle of matches and like I stole the Mr. Burns hop in.
    [10:27-10:29] at
    [10:30-10:36] I brought a tiny airplane to the ring and I told my opponent to hop in and I had three, 400 people chanting hop in at this guy.
    [10:36-10:39] How successful have you been?
    [10:39-10:40] It sounds like, is this growing?
    [10:40-10:45] Do you feel like Dan Hausen is becoming a bigger and bigger character?
    [10:45-10:47] Yes, absolutely.
    [10:47-10:53] Since I've switched this, which is about 2 years ago, and about a year full of doing this actual character, I've been wrestling for 8 years.
    [10:54-11:00] Once since I've switched this, it's just like snowballed more and more and more and now I have a shirt.
    [11:00-11:06] hot topic, and I've gotten signed to like a TV company, and they're just like, go do your weird stuff.
    [11:06-11:07] Like do it.
    [11:07-11:08] Have fun.
    [11:08-11:09] Be Danhausen.
    [11:09-11:10] That's that's what we need.
    [11:10-11:13] I want to be a part of Dan Hausen's world, you know?
    [11:13-11:14] Don't you see that, Matt?
    [11:14-11:18] I want to maybe do some sort of, I want to tape a video.
    [11:18-11:25] I seriously want to do something where you're in the ring and then I appear and I'm either for you or against you.
    [11:25-11:26] Do you know what I mean?
    [11:26-11:27] Or you're my long lost son.
    [11:28-11:00] Uh, we've, we've got a, we've got a,
    [11:30-11:31] somehow get into...
    [11:31-11:34] I want to get into the lore of Danhausen.
    [11:34-11:36] Do you know what I mean?
    [11:36-11:37] I wanna be part of it.
    [11:37-11:38] What would you do with me?
    [11:38-11:41] Oh, with you, I would call us both legendary lately.
    [11:42-11:44] Guess what?
    [11:45-11:45] There a lot of those now.
    [11:45-11:47] There's literally like 600 in America.
    [11:47-11:50] So he might want to come up with something cooler.
    [11:50-11:53] This character is all about himself.
    [11:53-11:55] He's all about making sacks of money, I call them.
    [11:55-11:58] I carry around a money sack.
    [11:58-12:00] I pulled it out after I won my contract.
    [12:00-12:03] And I revealed it from my cape.
    [12:03-12:10] I pulled a $20 bill and I said, look at these 1000000s and they threw it.
    [12:10-12:12] Would we actually fight?
    [12:13-12:17] And 1st of all, you know, I know how to handle myself.
    [12:17-12:18] Wait a minute.
    [12:18-12:20] Oh, come on, son. I'm fairly athletic.
    [12:20-12:25] I can take a punch and I love to fake fight.
    [12:26-12:29] And so if I entered the ring, would we start out being friends?
    [12:30-12:33] Then I would think that you had gotten too cocky and I would attack Dan Hausen.
    [12:33-12:33] What would happen?
    [12:34-12:45] Yeah, maybe. I think I do this thing where I try to punch people in the groin right before the bell rings, so I can just pin them without doing any work. So I don't think I would do that because people know that I love Conan.
    [12:45-12:48] Right. Like as a character, it's very public that I love Conan.
    [12:48-12:50] and that's one of Dan Hausen's idols.
    [12:50-12:52] Uh, so I don't think they would think that.
    [12:52-12:54] They'd probably be taken back if you did it.
    [12:54-12:54] Okay, how about this?
    [12:54-12:56] Let me pitch you this because I'm really into this.
    [12:56-12:59] All right, so Dan Hausen, you're fighting your foe.
    [13:00-13:01] He starts to get the better of you.
    [13:02-13:03] He starts to win.
    [13:03-13:06] He grabs the bag of teeth and starts to pour them into your mouth.
    [13:06-13:08] He steals your sock of money.
    [13:08-13:10] He punches you in the groin.
    [13:10-13:11] It's all going badly.
    [13:11-13:17] When all of a sudden, the music changes, fog machines go on, and I appear.
    [13:17-13:22] I come down on wires and it's me and I'm there to save Danhausen.
    [13:22-13:25] And, uh, I think the crowd would go nuts.
    [13:25-13:26] I hope so.
    [13:27-13:29] What if the crowd's just like, all right, okay.
    [13:30-13:31] Cool. Let's see what he's got.
    [13:31-13:32] All right, well, let's go.
    [13:32-13:35] If we go early, we can beat the traffic.
    [13:35-13:44] In my mind before I go on, that's what I think, that's the reaction I always think. If I leave now, I can beat the traffic.
    [13:44-13:47] I want in on the Danhausen world.
    [13:47-13:48] I really do, Donovan.
    [13:48-13:52] Yeah, well, I would love that if that's a possibility.
    [13:52-13:55] That's like the ultimate guest for Good Night Housing with Danhausen.
    [13:55-13:56] You know what?
    [13:56-13:59] I've always said, if there's a way that I can be involved,
    [14:00-14:05] with Good Nighthausen with Danhausen, I want Inhausen, and right now, Hausen.
    [14:05-14:08] Not tomorrow, Hausen, but today, Hausen.
    [14:08-14:10] I'm not fucking around, Hausen.
    [14:10-14:11] I'm serious, Hausen.
    [14:12-14:13] So let's make this happen, Hausen.
    [14:13-14:15] Let's sign a contract housing.
    [14:15-14:16] I want to get paid, housing.
    [14:16-14:20] Yes, we'll pay you in a wonderful sacks of human money.
    [14:20-14:24] That is only one kind of money.
    [14:24-14:27] There's only human money.
    [14:27-14:29] No animal uses money.
    [14:30-14:31] Yes, no, he has no idea.
    [14:31-14:33] Uh-huh, uh-huh. He just knows it gets you power.
    [14:33-14:34] Yeah, wow.
    [14:34-14:36] That's very exciting.
    [14:36-14:36] Well, you know what?
    [14:36-14:37] you're going to do well.
    [14:37-14:47] I love that you're going to Canada because I I, I don't say this just to suck up to Canada, but I love, I love Canadians, and I think they're like the funniest, one of the funniest countries in the world.
    [14:47-14:48] They're really funny people.
    [14:48-14:55] So I think, and they really love nuanced, like, weird kooky comedy, and they've always been so nice to me.
    [14:55-14:57] So I love that you're going to Montreal.
    [14:57-14:57] I that's great.
    [14:57-14:58] Thank you.
    [14:58-15:00] Yeah, it's been exciting and a lot.
    [15:00-15:05] Donovan, you have my blessing, and I will figure out a way to enter your the world of Danhausen.
    [15:06-15:06] I will.
    [15:06-15:07] Please do.
    [15:07-15:09] I would lose my mind and so would my fans.
    [15:09-15:10] It'd be crazy.
    [15:10-15:11] All right.
    [15:11-15:14] Well, Sona, we make sure... Oh, I will follow up on this.
    [15:14-15:16] Yes, we'll follow up on this.
    [15:16-15:17] I'm excited about it.
    [15:17-15:18] Yes.
    [15:18-15:19] Thank you.
    [15:19-15:20] Thank you for doing this.
    [15:20-15:21] Yeah, yeah, yeah.
    [15:21-15:21] No problem.
    [15:21-15:23] Hey, really nice to meet you, Donovan.
    [15:23-15:24] Nice meeting you too.
    [15:24-15:25] Nice meeting all of you.
    [15:25-15:25] Bye bye.
    [15:25-15:26] Thanks Donovan.
    [15:26-15:29] Conan O'Brien needs a fan.
    [15:29-15:30] With Conan O.
    [15:30-15:38] Ryan, Sonam of Session, and Matt Gorley, produced by me, Matt Gorley, executive produced by Adam Sachs, Jeff Ross, and Nick Leo.
    [15:38-15:40] Incidental music by Jimmy Vivino.
    [15:40-15:41] Take it away, Jimmy.
    [15:41-15:52] Supervising producer Aaron Blair, associate talent producer Jennifer Samples, associate producers Sean Doherty and Lisa Burm.
    [15:52-15:59] Engineering by Eduardo Perez, get three free months of SiriusXM when you sign up at Siriusxm.com slash Conan.
    [16:00-16:05] Please rate, review, and subscribe to Conan O'Brien needs a fan wherever Find Podcasts are downloaded.
    [16:11-16:17] Hey everyone, it's Kelly Ripa, and we're celebrating 3 years of my podcast.
    [16:17-16:18] Let's talk off camera.
    [16:18-16:29] No hair, no makeup, just 3 great years of the most honest conversations, real stories, and unfiltered talk, and we're joined every week by celebrity guests like Nikki Glaser.
    """

    // MARK: - Parser

    /// Parse the raw transcript into TranscriptChunks.
    /// Each non-blank line becomes one chunk in order of appearance.
    /// Tolerates lines of either form:
    ///   `[M:SS-M:SS] text`
    ///   `[M:SS-M:SS] (fast) text`
    static func parseChunks(assetId: String = ConanFanhausenRevisitedFixture.assetId) -> [TranscriptChunk] {
        var chunks: [TranscriptChunk] = []
        let lines = rawTranscript.split(whereSeparator: { $0 == "\n" })
        var index = 0
        for raw in lines {
            let line = raw.trimmingCharacters(in: .whitespaces)
            if line.isEmpty { continue }
            guard let parsed = parseLine(line) else { continue }
            // Tolerate degenerate timestamps (end < start) by clamping.
            let start = parsed.start
            let end = max(parsed.end, parsed.start)
            chunks.append(
                TranscriptChunk(
                    id: "\(assetId)-chunk-\(index)",
                    analysisAssetId: assetId,
                    segmentFingerprint: "fixture-fp-\(index)",
                    chunkIndex: index,
                    startTime: start,
                    endTime: end,
                    text: parsed.text,
                    normalizedText: parsed.text.lowercased(),
                    pass: "fast",
                    modelVersion: "fixture-v1",
                    transcriptVersion: nil,
                    atomOrdinal: nil
                )
            )
            index += 1
        }
        return chunks
    }

    private struct ParsedLine {
        let start: Double
        let end: Double
        let text: String
    }

    private static func parseLine(_ line: String) -> ParsedLine? {
        // Expect: "[M:SS-M:SS] [(fast) ]text"
        guard line.hasPrefix("[") else { return nil }
        guard let closing = line.firstIndex(of: "]") else { return nil }
        let timestamp = line[line.index(after: line.startIndex)..<closing]
        let parts = timestamp.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: true)
        guard parts.count == 2 else { return nil }
        guard let start = parseTimestamp(String(parts[0])),
              let end = parseTimestamp(String(parts[1])) else { return nil }

        var rest = line[line.index(after: closing)...].trimmingCharacters(in: .whitespaces)
        // Strip optional "(fast) " or "(final) " prefix
        if rest.hasPrefix("(") {
            if let close = rest.firstIndex(of: ")") {
                rest = String(rest[rest.index(after: close)...]).trimmingCharacters(in: .whitespaces)
            }
        }
        return ParsedLine(start: start, end: end, text: rest)
    }

    /// Parse "M:SS" or "MM:SS" into seconds.
    private static func parseTimestamp(_ s: String) -> Double? {
        let parts = s.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 2,
              let m = Int(parts[0]),
              let sec = Int(parts[1]) else { return nil }
        return Double(m * 60 + sec)
    }
}

