#!/usr/bin/env python3
"""
Local browser GUI for playhead-l2f.3 audio review.

Run from the repo root:

    python3 scripts/l2f-review-gui.py --host 0.0.0.0

Then open the printed LAN URL from another device on the same network.
Reviewed decisions are written only under TestFixtures/Corpus/Drafts.
"""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import posixpath
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CORPUS_DIR = REPO_ROOT / "TestFixtures" / "Corpus"
DRAFTS_DIR = CORPUS_DIR / "Drafts"
AUDIO_DIR = CORPUS_DIR / "Audio"
CODEX_REVIEW = DRAFTS_DIR / "codex-transcript-review.json"
DEFAULT_REVIEW_FILE = DRAFTS_DIR / "l2f-audio-review.json"
AUDIO_EXTENSIONS = {".m4a", ".mp3", ".mp4", ".aac", ".wav", ".flac"}


HTML_PAGE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>L2F Audio Review</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f7f4;
      --panel: #ffffff;
      --ink: #20211f;
      --muted: #6d7069;
      --line: #d8d9d2;
      --accent: #0f766e;
      --accent-2: #2563eb;
      --warn: #a16207;
      --bad: #b91c1c;
      --ok: #15803d;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      min-height: 100vh;
    }
    button, input, select, textarea {
      font: inherit;
    }
    button {
      min-height: 44px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--ink);
      padding: 8px 12px;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: white;
      font-weight: 650;
    }
    button.secondary {
      background: #eef6f4;
      border-color: #b8d8d3;
      color: #0b514b;
    }
    button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .app {
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      min-height: 100vh;
    }
    .sidebar {
      border-right: 1px solid var(--line);
      background: #eeeee8;
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }
    .topbar {
      padding: 14px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 10px;
    }
    h1 {
      font-size: 18px;
      margin: 0;
      line-height: 1.2;
    }
    .meta {
      color: var(--muted);
      font-size: 13px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .progress {
      height: 8px;
      border-radius: 999px;
      overflow: hidden;
      background: #d7d7cf;
    }
    .progress > div {
      height: 100%;
      background: var(--accent);
      width: 0%;
    }
    .filters {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .filters input,
    .filters select,
    input,
    select,
    textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--ink);
      min-height: 42px;
      padding: 8px 10px;
    }
    textarea {
      min-height: 120px;
      resize: vertical;
    }
    .entry-list {
      overflow: auto;
      padding: 8px;
      display: grid;
      gap: 6px;
    }
    .entry {
      border: 1px solid transparent;
      background: rgba(255, 255, 255, 0.72);
      border-radius: 8px;
      padding: 10px;
      display: grid;
      gap: 5px;
      text-align: left;
    }
    .entry.active {
      border-color: var(--accent);
      background: #ffffff;
    }
    .entry-title {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      font-weight: 650;
      min-width: 0;
    }
    .entry-title span:first-child {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      background: #e7e7df;
      color: #44463f;
      white-space: nowrap;
    }
    .chip.ok { background: #dcfce7; color: var(--ok); }
    .chip.warn { background: #fef3c7; color: var(--warn); }
    .chip.bad { background: #fee2e2; color: var(--bad); }
    .entry-sub {
      color: var(--muted);
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    main {
      min-width: 0;
      padding: 18px;
      display: grid;
      gap: 14px;
      align-content: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      display: grid;
      gap: 12px;
    }
    .panel h2 {
      margin: 0;
      font-size: 17px;
      line-height: 1.25;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .summary.compact {
      grid-template-columns: repeat(6, minmax(0, 1fr));
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fbfbf8;
    }
    .stat b {
      display: block;
      font-size: 20px;
      line-height: 1;
      margin-bottom: 4px;
    }
    .stat span {
      color: var(--muted);
      font-size: 12px;
    }
    .dashboard {
      display: grid;
      gap: 12px;
    }
    .progress-columns {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .progress-group {
      display: grid;
      gap: 7px;
      min-width: 0;
    }
    .progress-group h3 {
      font-size: 13px;
      margin: 0;
      color: var(--muted);
    }
    .progress-list {
      display: grid;
      gap: 6px;
      max-height: 184px;
      overflow: auto;
    }
    .progress-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      align-items: center;
      border-top: 1px solid #ecede7;
      padding-top: 6px;
      font-size: 12px;
      color: var(--muted);
    }
    .progress-row b {
      color: var(--ink);
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 13px;
      font-weight: 650;
    }
    audio {
      width: 100%;
    }
    .row {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .row.three {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .row.four {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }
    label span {
      color: var(--muted);
    }
    .toolbar {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .quick-actions {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
    }
    .quick-actions button {
      min-height: 48px;
      font-weight: 650;
    }
    .note {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .candidate-notes {
      border-left: 3px solid var(--line);
      padding-left: 10px;
      color: #4a4d46;
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .footer-state {
      color: var(--muted);
      font-size: 12px;
    }
    @media (max-width: 820px) {
      .app {
        grid-template-columns: 1fr;
      }
      .sidebar {
        min-height: auto;
        max-height: 44vh;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      main {
        padding: 12px;
      }
      .row,
      .row.three,
      .row.four {
        grid-template-columns: 1fr;
      }
      .summary,
      .summary.compact {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .progress-columns {
        grid-template-columns: 1fr;
      }
      .filters {
        grid-template-columns: 1fr;
      }
      button,
      input,
      select,
      textarea {
        font-size: 16px;
      }
      button {
        min-height: 50px;
      }
      .quick-actions {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .audio-panel {
        position: sticky;
        top: 0;
        z-index: 10;
        box-shadow: 0 8px 18px rgba(0, 0, 0, 0.08);
      }
      .action-toolbar {
        position: sticky;
        bottom: 0;
        z-index: 11;
        background: var(--panel);
        border-top: 1px solid var(--line);
        padding: 10px 0 0;
        margin: 0 -12px;
        padding-left: 12px;
        padding-right: 12px;
      }
      .action-toolbar button {
        flex: 1 1 calc(50% - 8px);
      }
      .panel {
        padding: 12px;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <div class="topbar">
        <h1>L2F Audio Review</h1>
        <div class="meta">
          <span id="queueName">Loading</span>
          <span id="reviewPath"></span>
        </div>
        <div class="progress" aria-label="Review progress"><div id="progressBar"></div></div>
        <div class="meta" id="progressText"></div>
        <div class="filters">
          <input id="searchInput" type="search" placeholder="Episode or advertiser">
          <select id="statusFilter">
            <option value="all">All statuses</option>
            <option value="unreviewed">Unreviewed</option>
            <option value="verified_ad">Verified ads</option>
            <option value="false_positive">False positives</option>
            <option value="zero_ad_confirmed">Zero-ad confirmed</option>
            <option value="unsure">Unsure</option>
          </select>
        </div>
      </div>
      <div class="entry-list" id="entryList"></div>
    </aside>
    <main>
      <section class="panel">
        <div class="summary">
          <div class="stat"><b id="statTotal">0</b><span>Total</span></div>
          <div class="stat"><b id="statDone">0</b><span>Reviewed</span></div>
          <div class="stat"><b id="statAds">0</b><span>Verified ads</span></div>
          <div class="stat"><b id="statOpen">0</b><span>Open</span></div>
        </div>
        <div class="dashboard">
          <div class="summary compact" id="statusSummary"></div>
          <div class="toolbar">
            <button id="nextOpenBtn" type="button">Next open</button>
            <span class="note" id="nextOpenText"></span>
          </div>
          <div class="progress-columns">
            <div class="progress-group">
              <h3>Episode Progress</h3>
              <div class="progress-list" id="episodeProgress"></div>
            </div>
            <div class="progress-group">
              <h3>Category Progress</h3>
              <div class="progress-list" id="categoryProgress"></div>
            </div>
          </div>
        </div>
      </section>

      <section class="panel audio-panel" id="detailPanel">
        <h2 id="detailTitle">Select an entry</h2>
        <div class="note" id="detailSubtitle"></div>
        <audio id="audio" preload="metadata" controls></audio>
        <div class="toolbar">
          <button id="playContextBtn">Play context</button>
          <button id="setStartBtn">Set start</button>
          <button id="setEndBtn">Set end</button>
          <button id="skipBackBtn">-5s</button>
          <button id="skipForwardBtn">+5s</button>
        </div>
        <div class="candidate-notes" id="candidateNotes"></div>
      </section>

      <section class="panel">
        <div class="quick-actions" aria-label="Quick decision shortcuts">
          <button type="button" data-quick-status="verified_ad">Verified Ad</button>
          <button type="button" data-quick-status="false_positive">False Positive</button>
          <button type="button" data-quick-status="zero_ad_confirmed">Zero-Ad</button>
          <button type="button" data-quick-status="unsure">Unsure</button>
        </div>
        <div class="row three">
          <label><span>Decision</span>
            <select id="status">
              <option value="unreviewed">Unreviewed</option>
              <option value="verified_ad">Verified ad</option>
              <option value="false_positive">False positive</option>
              <option value="zero_ad_confirmed">Zero-ad confirmed</option>
              <option value="unsure">Unsure</option>
            </select>
          </label>
          <label><span>Ad type</span>
            <select id="adType">
              <option value="">Review needed</option>
              <option value="host_read">Host read</option>
              <option value="dynamic_insertion">Dynamic insertion</option>
              <option value="blended_host_read">Blended host read</option>
              <option value="produced_segment">Produced segment</option>
              <option value="promo">Promo</option>
            </select>
          </label>
          <label><span>Transition</span>
            <select id="transitionType">
              <option value="">Review needed</option>
              <option value="explicit">Explicit</option>
              <option value="musical">Musical</option>
              <option value="hard_cut">Hard cut</option>
              <option value="blended">Blended</option>
            </select>
          </label>
        </div>
        <div class="row four">
          <label><span>Start seconds</span><input id="startSeconds" type="number" step="0.1"></label>
          <label><span>End seconds</span><input id="endSeconds" type="number" step="0.1"></label>
          <label><span>Advertiser</span><input id="advertiser" type="text"></label>
          <label><span>Product</span><input id="product" type="text"></label>
        </div>
        <div class="row">
          <label><span>Boundary confidence</span>
            <select id="boundaryConfidence">
              <option value="">Review needed</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </label>
          <label><span>Reviewer</span><input id="reviewer" type="text" placeholder="Optional"></label>
        </div>
        <label><span>Review notes</span><textarea id="reviewNotes"></textarea></label>
        <div class="toolbar action-toolbar">
          <button class="primary" id="saveBtn">Save</button>
          <button class="secondary" id="saveNextBtn">Save and next</button>
          <button id="prevBtn">Previous</button>
          <button id="nextBtn">Next</button>
          <button id="exportBtn">Write episode review files</button>
        </div>
        <div class="footer-state" id="saveState"></div>
      </section>
    </main>
  </div>
  <script>
    const state = {
      entries: [],
      reviews: {},
      selectedIndex: 0,
      filter: 'all',
      search: ''
    };

    const els = {
      queueName: document.getElementById('queueName'),
      reviewPath: document.getElementById('reviewPath'),
      progressBar: document.getElementById('progressBar'),
      progressText: document.getElementById('progressText'),
      statTotal: document.getElementById('statTotal'),
      statDone: document.getElementById('statDone'),
      statAds: document.getElementById('statAds'),
      statOpen: document.getElementById('statOpen'),
      statusSummary: document.getElementById('statusSummary'),
      episodeProgress: document.getElementById('episodeProgress'),
      categoryProgress: document.getElementById('categoryProgress'),
      nextOpenBtn: document.getElementById('nextOpenBtn'),
      nextOpenText: document.getElementById('nextOpenText'),
      entryList: document.getElementById('entryList'),
      searchInput: document.getElementById('searchInput'),
      statusFilter: document.getElementById('statusFilter'),
      detailTitle: document.getElementById('detailTitle'),
      detailSubtitle: document.getElementById('detailSubtitle'),
      audio: document.getElementById('audio'),
      candidateNotes: document.getElementById('candidateNotes'),
      status: document.getElementById('status'),
      adType: document.getElementById('adType'),
      transitionType: document.getElementById('transitionType'),
      startSeconds: document.getElementById('startSeconds'),
      endSeconds: document.getElementById('endSeconds'),
      advertiser: document.getElementById('advertiser'),
      product: document.getElementById('product'),
      boundaryConfidence: document.getElementById('boundaryConfidence'),
      reviewer: document.getElementById('reviewer'),
      reviewNotes: document.getElementById('reviewNotes'),
      saveState: document.getElementById('saveState')
    };

    function currentEntry() {
      return state.entries[state.selectedIndex] || null;
    }

    function reviewFor(entry) {
      return (entry && state.reviews[entry.id]) || {};
    }

    function mergedReview(entry) {
      const review = reviewFor(entry);
      return {
        status: review.status || 'unreviewed',
        start_seconds: review.start_seconds ?? entry.start_seconds ?? '',
        end_seconds: review.end_seconds ?? entry.end_seconds ?? '',
        advertiser: review.advertiser ?? entry.advertiser_guess ?? '',
        product: review.product ?? entry.product_guess ?? '',
        ad_type: review.ad_type ?? entry.ad_type ?? '',
        transition_type: review.transition_type ?? entry.transition_type ?? '',
        boundary_confidence: review.boundary_confidence ?? '',
        reviewer: review.reviewer ?? '',
        notes: review.notes ?? ''
      };
    }

    function statusClass(status, trap) {
      if (status === 'verified_ad') return 'ok';
      if (status === 'false_positive' || status === 'zero_ad_confirmed') return 'bad';
      if (status === 'unsure') return 'warn';
      return trap ? 'warn' : '';
    }

    function filteredEntries() {
      const query = state.search.trim().toLowerCase();
      return state.entries
        .map((entry, index) => ({ entry, index }))
        .filter(({ entry }) => {
          const status = reviewFor(entry).status || 'unreviewed';
          if (state.filter !== 'all' && status !== state.filter) return false;
          if (!query) return true;
          const haystack = [
            entry.episode_id,
            entry.advertiser_guess,
            entry.product_guess,
            entry.notes,
            entry.id
          ].filter(Boolean).join(' ').toLowerCase();
          return haystack.includes(query);
        });
    }

    function formatSeconds(value) {
      if (value === null || value === undefined || value === '') return 'n/a';
      const number = Number(value);
      if (!Number.isFinite(number)) return 'n/a';
      return number.toFixed(1);
    }

    function categoryFor(entry) {
      return entry.category || entry.corpus_category || entry.ad_type || (entry.false_positive_trap ? 'zero_or_false_positive_trap' : 'unknown');
    }

    function progressData() {
      const statuses = ['unreviewed', 'verified_ad', 'false_positive', 'zero_ad_confirmed', 'unsure'];
      const byStatus = Object.fromEntries(statuses.map(status => [status, 0]));
      const episodes = new Map();
      const categories = new Map();
      let missingAudio = 0;
      for (const entry of state.entries) {
        const status = reviewFor(entry).status || 'unreviewed';
        byStatus[status] = (byStatus[status] || 0) + 1;
        if (!entry.audio_available) missingAudio += 1;

        const episode = episodes.get(entry.episode_id) || { name: entry.episode_id, total: 0, done: 0, ads: 0 };
        episode.total += 1;
        if (status !== 'unreviewed') episode.done += 1;
        if (status === 'verified_ad') episode.ads += 1;
        episodes.set(entry.episode_id, episode);

        const categoryName = categoryFor(entry);
        const category = categories.get(categoryName) || { name: categoryName, total: 0, done: 0, ads: 0 };
        category.total += 1;
        if (status !== 'unreviewed') category.done += 1;
        if (status === 'verified_ad') category.ads += 1;
        categories.set(categoryName, category);
      }
      const total = state.entries.length;
      const done = total - (byStatus.unreviewed || 0);
      return {
        total,
        done,
        open: byStatus.unreviewed || 0,
        ads: byStatus.verified_ad || 0,
        missingAudio,
        percent: total ? Math.round((done / total) * 100) : 0,
        byStatus,
        episodes: [...episodes.values()].sort((a, b) => (b.total - b.done) - (a.total - a.done) || a.name.localeCompare(b.name)),
        categories: [...categories.values()].sort((a, b) => (b.total - b.done) - (a.total - a.done) || a.name.localeCompare(b.name))
      };
    }

    function renderProgressList(container, rows) {
      container.innerHTML = '';
      if (!rows.length) {
        container.innerHTML = '<div class="note">No entries</div>';
        return;
      }
      for (const row of rows) {
        const pct = row.total ? Math.round((row.done / row.total) * 100) : 0;
        const item = document.createElement('div');
        item.className = 'progress-row';
        item.innerHTML = `
          <div>
            <b>${htmlEscape(row.name)}</b>
            <div class="progress" aria-label="${htmlEscape(row.name)} progress"><div style="width:${pct}%"></div></div>
          </div>
          <span>${row.done}/${row.total}</span>
        `;
        container.appendChild(item);
      }
    }

    function nextOpenIndex(start = state.selectedIndex + 1) {
      if (!state.entries.length) return null;
      for (let offset = 0; offset < state.entries.length; offset += 1) {
        const index = (start + offset) % state.entries.length;
        const status = reviewFor(state.entries[index]).status || 'unreviewed';
        if (status === 'unreviewed') return index;
      }
      return null;
    }

    function renderProgress() {
      const progress = progressData();
      els.progressBar.style.width = `${progress.percent}%`;
      els.progressText.textContent = `${progress.done} / ${progress.total} reviewed (${progress.percent}%)`;
      els.statTotal.textContent = progress.total;
      els.statDone.textContent = progress.done;
      els.statAds.textContent = progress.ads;
      els.statOpen.textContent = progress.open;
      const statusRows = [
        ['verified_ad', 'Ads'],
        ['false_positive', 'False'],
        ['zero_ad_confirmed', 'Zero'],
        ['unsure', 'Unsure'],
        ['unreviewed', 'Open'],
        ['missing_audio', 'No Audio']
      ];
      els.statusSummary.innerHTML = statusRows.map(([key, label]) => {
        const value = key === 'missing_audio' ? progress.missingAudio : (progress.byStatus[key] || 0);
        return `<div class="stat"><b>${value}</b><span>${label}</span></div>`;
      }).join('');
      renderProgressList(els.episodeProgress, progress.episodes);
      renderProgressList(els.categoryProgress, progress.categories);
      const next = nextOpenIndex(state.selectedIndex + 1);
      els.nextOpenText.textContent = next === null ? 'All entries reviewed' : `${state.entries[next].episode_id} #${state.entries[next].candidate_index}`;
      els.nextOpenBtn.disabled = next === null;
    }

    function renderList() {
      els.entryList.innerHTML = '';
      for (const { entry, index } of filteredEntries()) {
        const review = reviewFor(entry);
        const status = review.status || 'unreviewed';
        const row = document.createElement('button');
        row.className = `entry ${index === state.selectedIndex ? 'active' : ''}`;
        row.type = 'button';
        row.onclick = () => selectIndex(index);
        row.innerHTML = `
          <div class="entry-title">
            <span>${htmlEscape(entry.episode_id)} #${entry.candidate_index}</span>
            <span class="chip ${statusClass(status, entry.false_positive_trap)}">${labelForStatus(status)}</span>
          </div>
          <div class="entry-sub">${entry.false_positive_trap ? 'false-positive trap' : `${formatSeconds(entry.start_seconds)}-${formatSeconds(entry.end_seconds)}`} · ${htmlEscape(entry.advertiser_guess || 'review needed')}</div>
        `;
        els.entryList.appendChild(row);
      }
    }

    function labelForStatus(status) {
      return {
        unreviewed: 'open',
        verified_ad: 'ad',
        false_positive: 'false',
        zero_ad_confirmed: 'zero',
        unsure: 'unsure'
      }[status] || status;
    }

    function selectIndex(index) {
      state.selectedIndex = Math.max(0, Math.min(index, state.entries.length - 1));
      render();
    }

    function renderDetail() {
      const entry = currentEntry();
      if (!entry) return;
      const review = mergedReview(entry);
      els.detailTitle.textContent = `${entry.episode_id} #${entry.candidate_index}`;
      els.detailSubtitle.textContent = `${entry.source} · context ${formatSeconds(entry.context_start_seconds)}-${formatSeconds(entry.context_end_seconds)}`;
      els.candidateNotes.textContent = entry.notes || '';
      els.status.value = review.status;
      els.adType.value = review.ad_type || '';
      els.transitionType.value = review.transition_type || '';
      els.startSeconds.value = review.start_seconds;
      els.endSeconds.value = review.end_seconds;
      els.advertiser.value = review.advertiser;
      els.product.value = review.product;
      els.boundaryConfidence.value = review.boundary_confidence || '';
      els.reviewer.value = review.reviewer || '';
      els.reviewNotes.value = review.notes || '';
      els.audio.src = entry.audio_url || '';
      els.audio.disabled = !entry.audio_url;
      els.saveState.textContent = entry.audio_available ? 'Audio ready' : 'No matching audio file found';
    }

    function render() {
      renderProgress();
      renderList();
      renderDetail();
    }

    function htmlEscape(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function collectReview() {
      const numberOrNull = (value) => {
        if (value === '') return null;
        const number = Number(value);
        return Number.isFinite(number) ? Math.round(number * 10) / 10 : null;
      };
      return {
        status: els.status.value,
        start_seconds: numberOrNull(els.startSeconds.value),
        end_seconds: numberOrNull(els.endSeconds.value),
        advertiser: els.advertiser.value.trim() || null,
        product: els.product.value.trim() || null,
        ad_type: els.adType.value || null,
        transition_type: els.transitionType.value || null,
        boundary_confidence: els.boundaryConfidence.value || null,
        reviewer: els.reviewer.value.trim() || null,
        notes: els.reviewNotes.value.trim() || null
      };
    }

    async function saveReview(moveNext) {
      const entry = currentEntry();
      if (!entry) return;
      const review = collectReview();
      els.saveState.textContent = 'Saving';
      const response = await fetch('/api/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entry_id: entry.id, review })
      });
      if (!response.ok) {
        els.saveState.textContent = `Save failed: ${await response.text()}`;
        return;
      }
      const payload = await response.json();
      state.reviews = payload.reviews || state.reviews;
      els.saveState.textContent = 'Saved';
      if (moveNext) {
        const next = nextOpenIndex(state.selectedIndex + 1);
        selectIndex(next ?? state.selectedIndex + 1);
      }
      else render();
    }

    async function exportReviews() {
      els.saveState.textContent = 'Writing episode review files';
      const response = await fetch('/api/export', { method: 'POST' });
      if (!response.ok) {
        els.saveState.textContent = `Export failed: ${await response.text()}`;
        return;
      }
      const payload = await response.json();
      els.saveState.textContent = `Wrote ${payload.files.length} episode review files`;
    }

    function playContext() {
      const entry = currentEntry();
      if (!entry || !entry.audio_url) return;
      const start = Number(entry.context_start_seconds ?? entry.start_seconds ?? 0);
      els.audio.currentTime = Number.isFinite(start) ? start : 0;
      els.audio.play();
    }

    function setBoundary(which) {
      const value = Number(els.audio.currentTime || 0).toFixed(1);
      if (which === 'start') els.startSeconds.value = value;
      if (which === 'end') els.endSeconds.value = value;
    }

    function applyQuickStatus(status) {
      els.status.value = status;
      if (status === 'verified_ad' && !els.boundaryConfidence.value) {
        els.boundaryConfidence.value = 'medium';
      }
      els.saveState.textContent = `Marked ${labelForStatus(status)}; save when ready`;
    }

    async function loadState() {
      const response = await fetch('/api/state');
      if (!response.ok) {
        document.body.innerHTML = `<pre>${htmlEscape(await response.text())}</pre>`;
        return;
      }
      const payload = await response.json();
      state.entries = payload.entries || [];
      state.reviews = payload.reviews || {};
      els.queueName.textContent = payload.queue_path || 'queue';
      els.reviewPath.textContent = payload.review_path || '';
      render();
    }

    els.searchInput.addEventListener('input', () => {
      state.search = els.searchInput.value;
      renderList();
    });
    els.statusFilter.addEventListener('change', () => {
      state.filter = els.statusFilter.value;
      renderList();
    });
    document.getElementById('playContextBtn').onclick = playContext;
    document.getElementById('setStartBtn').onclick = () => setBoundary('start');
    document.getElementById('setEndBtn').onclick = () => setBoundary('end');
    document.getElementById('skipBackBtn').onclick = () => { els.audio.currentTime = Math.max(0, els.audio.currentTime - 5); };
    document.getElementById('skipForwardBtn').onclick = () => { els.audio.currentTime = els.audio.currentTime + 5; };
    document.getElementById('saveBtn').onclick = () => saveReview(false);
    document.getElementById('saveNextBtn').onclick = () => saveReview(true);
    document.getElementById('prevBtn').onclick = () => selectIndex(state.selectedIndex - 1);
    document.getElementById('nextBtn').onclick = () => selectIndex(state.selectedIndex + 1);
    document.getElementById('exportBtn').onclick = exportReviews;
    els.nextOpenBtn.onclick = () => {
      const next = nextOpenIndex(state.selectedIndex + 1);
      if (next !== null) selectIndex(next);
    };
    for (const button of document.querySelectorAll('[data-quick-status]')) {
      button.addEventListener('click', () => applyQuickStatus(button.dataset.quickStatus));
    }

    loadState();
  </script>
</body>
</html>
"""


@dataclass
class AppConfig:
    host: str
    port: int
    queue_path: Path
    review_path: Path
    auto_generate_queue: bool


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def safe_name(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value)
    safe = safe.strip("._-")
    return safe or "episode"


def find_audio(episode_id: str) -> Path | None:
    for child in AUDIO_DIR.iterdir():
        if child.is_file() and child.suffix.lower() in AUDIO_EXTENSIONS and child.stem == episode_id:
            return child
    return None


def choose_queue_path(explicit: str | None, auto_generate: bool) -> Path:
    if explicit:
        return resolve_repo_path(explicit)
    preferred = DRAFTS_DIR / "review-queue.json"
    codex_queue = DRAFTS_DIR / "codex-review-queue.json"
    if preferred.exists():
        return preferred
    if codex_queue.exists():
        return codex_queue
    if auto_generate and CODEX_REVIEW.exists():
        subprocess.run(
            [
                "swift",
                "scripts/l2f-draft-annotation.swift",
                "--review-queue-only",
                "--review-source",
                "TestFixtures/Corpus/Drafts/codex-transcript-review.json",
                "--review-queue-name",
                "codex-review-queue",
            ],
            cwd=str(REPO_ROOT),
            check=True,
        )
        return codex_queue
    return preferred


def resolve_repo_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def ensure_drafts_path(path: Path) -> None:
    resolved = path.resolve()
    drafts = DRAFTS_DIR.resolve()
    if resolved != drafts and drafts not in resolved.parents:
        raise ValueError(f"{resolved} is not under {drafts}")


def load_queue_entries(queue_path: Path) -> list[dict[str, Any]]:
    queue = load_json(queue_path)
    if not isinstance(queue, dict) or not isinstance(queue.get("entries"), list):
        raise ValueError(f"{queue_path} does not contain review queue entries")
    entries: list[dict[str, Any]] = []
    for raw in queue["entries"]:
        if not isinstance(raw, dict):
            continue
        entry = dict(raw)
        entry_id = str(entry.get("id") or f"{entry.get('episode_id', 'episode')}#{entry.get('candidate_index', len(entries) + 1)}")
        episode_id = str(entry.get("episode_id") or entry_id.split("#")[0])
        audio = find_audio(episode_id)
        entry["id"] = entry_id
        entry["episode_id"] = episode_id
        entry["audio_available"] = audio is not None
        entry["audio_url"] = f"/api/audio/{urllib.parse.quote(entry_id, safe='')}" if audio else None
        entries.append(entry)
    return entries


def load_reviews(review_path: Path) -> dict[str, Any]:
    payload = load_json(review_path, default={})
    if not isinstance(payload, dict):
        return {}
    reviews = payload.get("reviews")
    return reviews if isinstance(reviews, dict) else {}


def save_reviews(review_path: Path, queue_path: Path, reviews: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema": "playhead-l2f-audio-review-v1",
        "queue_path": str(queue_path.relative_to(REPO_ROOT)) if queue_path.is_relative_to(REPO_ROOT) else str(queue_path),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "human_audio_verification_required": True,
        "reviews": reviews,
    }
    write_json_atomic(review_path, payload)
    return payload


def grouped_episode_reviews(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        review = reviews.get(entry["id"], {})
        merged = dict(entry)
        merged["review"] = review
        grouped.setdefault(entry["episode_id"], []).append(merged)
    return grouped


def entry_category(entry: dict[str, Any]) -> str:
    for key in ("category", "corpus_category", "ad_type"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if entry.get("false_positive_trap") is True:
        return "zero_or_false_positive_trap"
    return "unknown"


def review_status(entry: dict[str, Any], reviews: dict[str, Any]) -> str:
    review = reviews.get(entry["id"])
    if isinstance(review, dict):
        status = review.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
    return "unreviewed"


def progress_summary(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, Any]:
    by_status: dict[str, int] = {
        "unreviewed": 0,
        "verified_ad": 0,
        "false_positive": 0,
        "zero_ad_confirmed": 0,
        "unsure": 0,
    }
    by_episode: dict[str, dict[str, Any]] = {}
    by_category: dict[str, dict[str, Any]] = {}
    missing_audio = 0
    next_open: dict[str, Any] | None = None

    for entry in entries:
        status = review_status(entry, reviews)
        by_status[status] = by_status.get(status, 0) + 1
        if not entry.get("audio_available"):
            missing_audio += 1
        if status == "unreviewed" and next_open is None:
            next_open = {
                "id": entry["id"],
                "episode_id": entry["episode_id"],
                "candidate_index": entry.get("candidate_index"),
            }

        episode = by_episode.setdefault(
            entry["episode_id"],
            {"name": entry["episode_id"], "total": 0, "done": 0, "ads": 0},
        )
        category_name = entry_category(entry)
        category = by_category.setdefault(
            category_name,
            {"name": category_name, "total": 0, "done": 0, "ads": 0},
        )
        for bucket in (episode, category):
            bucket["total"] += 1
            if status != "unreviewed":
                bucket["done"] += 1
            if status == "verified_ad":
                bucket["ads"] += 1

    total = len(entries)
    done = total - by_status.get("unreviewed", 0)

    def sorted_rows(rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            rows.values(),
            key=lambda item: (-(item["total"] - item["done"]), item["name"]),
        )

    return {
        "total": total,
        "done": done,
        "open": by_status.get("unreviewed", 0),
        "ads": by_status.get("verified_ad", 0),
        "missing_audio": missing_audio,
        "percent": round((done / total) * 100) if total else 0,
        "by_status": by_status,
        "episodes": sorted_rows(by_episode),
        "categories": sorted_rows(by_category),
        "next_open": next_open,
    }


def best_lan_ip() -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())
    finally:
        sock.close()


def parse_range(header: str | None, size: int) -> tuple[int, int, bool]:
    if not header or not header.startswith("bytes="):
        return 0, size - 1, False
    spec = header[6:].split(",", 1)[0].strip()
    if "-" not in spec:
        return 0, size - 1, False
    start_raw, end_raw = spec.split("-", 1)
    try:
        if start_raw == "":
            length = int(end_raw)
            return max(0, size - length), size - 1, True
        start = int(start_raw)
        end = int(end_raw) if end_raw else size - 1
        return max(0, start), min(size - 1, end), True
    except ValueError:
        return 0, size - 1, False


class L2FReviewHandler(BaseHTTPRequestHandler):
    server_version = "L2FReviewGUI/1.0"

    @property
    def config(self) -> AppConfig:
        return self.server.config  # type: ignore[attr-defined]

    @property
    def entries(self) -> list[dict[str, Any]]:
        return self.server.entries  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_html(HTML_PAGE)
            return
        if parsed.path == "/api/state":
            self.send_state()
            return
        if parsed.path.startswith("/api/audio/"):
            entry_id = urllib.parse.unquote(parsed.path.removeprefix("/api/audio/"))
            self.send_audio(entry_id)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/review":
            self.save_review()
            return
        if parsed.path == "/api/export":
            self.export_episode_reviews()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def send_html(self, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, value: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(value, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def read_json_body(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length)
        return json.loads(data.decode("utf-8"))

    def send_state(self) -> None:
        reviews = load_reviews(self.config.review_path)
        self.send_json(
            {
                "queue_path": str(self.config.queue_path.relative_to(REPO_ROOT)),
                "review_path": str(self.config.review_path.relative_to(REPO_ROOT)),
                "entries": self.entries,
                "reviews": reviews,
                "progress": progress_summary(self.entries, reviews),
            }
        )

    def save_review(self) -> None:
        try:
            body = self.read_json_body()
            entry_id = str(body["entry_id"])
            review = body["review"]
            if not isinstance(review, dict):
                raise ValueError("review must be an object")
            if entry_id not in {entry["id"] for entry in self.entries}:
                raise ValueError(f"unknown entry id: {entry_id}")
            review["reviewed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            reviews = load_reviews(self.config.review_path)
            reviews[entry_id] = review
            save_reviews(self.config.review_path, self.config.queue_path, reviews)
            self.send_json({"ok": True, "reviews": reviews})
        except Exception as exc:
            self.send_error(HTTPStatus.BAD_REQUEST, explain=html.escape(str(exc)))

    def export_episode_reviews(self) -> None:
        try:
            reviews = load_reviews(self.config.review_path)
            grouped = grouped_episode_reviews(self.entries, reviews)
            files: list[str] = []
            for episode_id, episode_entries in grouped.items():
                path = DRAFTS_DIR / f"{safe_name(episode_id)}.audio-review.json"
                payload = {
                    "schema": "playhead-l2f-episode-audio-review-v1",
                    "episode_id": episode_id,
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "human_audio_verification_required": True,
                    "entries": episode_entries,
                }
                write_json_atomic(path, payload)
                files.append(str(path.relative_to(REPO_ROOT)))
            self.send_json({"ok": True, "files": files})
        except Exception as exc:
            self.send_error(HTTPStatus.BAD_REQUEST, explain=html.escape(str(exc)))

    def send_audio(self, entry_id: str) -> None:
        entry = next((item for item in self.entries if item["id"] == entry_id), None)
        if not entry:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        audio = find_audio(entry["episode_id"])
        if not audio:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        size = audio.stat().st_size
        start, end, partial = parse_range(self.headers.get("Range"), size)
        if start > end:
            self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            return
        mime = mimetypes.guess_type(str(audio))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.PARTIAL_CONTENT if partial else HTTPStatus.OK)
        self.send_header("Content-Type", mime)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        with audio.open("rb") as handle:
            handle.seek(start)
            shutil.copyfileobj(_LimitedReader(handle, end - start + 1), self.wfile)


class _LimitedReader:
    def __init__(self, handle: Any, remaining: int) -> None:
        self.handle = handle
        self.remaining = remaining

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if size < 0 or size > self.remaining:
            size = self.remaining
        data = self.handle.read(size)
        self.remaining -= len(data)
        return data


class L2FReviewServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], config: AppConfig) -> None:
        self.config = config
        self.entries = load_queue_entries(config.queue_path)
        super().__init__(address, L2FReviewHandler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the L2F manual audio review GUI.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Use 0.0.0.0 for iPhone/LAN access.")
    parser.add_argument("--port", type=int, default=8765, help="Bind port.")
    parser.add_argument("--queue", help="Review queue JSON path. Defaults to Drafts review-queue/codex-review-queue.")
    parser.add_argument("--review-file", default=str(DEFAULT_REVIEW_FILE), help="Drafts JSON file for saved decisions.")
    parser.add_argument("--no-generate-queue", action="store_true", help="Do not auto-generate the Codex review queue when missing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    review_path = resolve_repo_path(args.review_file)
    try:
        ensure_drafts_path(review_path)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        return 2
    try:
        queue_path = choose_queue_path(args.queue, not args.no_generate_queue)
        if not queue_path.exists():
            print(f"error: review queue not found at {queue_path}", file=sys.stderr, flush=True)
            print("Generate one with: swift scripts/l2f-draft-annotation.swift --review-queue-only --review-source TestFixtures/Corpus/Drafts/codex-transcript-review.json", file=sys.stderr, flush=True)
            return 2
        config = AppConfig(
            host=args.host,
            port=args.port,
            queue_path=queue_path.resolve(),
            review_path=review_path.resolve(),
            auto_generate_queue=not args.no_generate_queue,
        )
        server = L2FReviewServer((args.host, args.port), config)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        return 1

    bound_host, bound_port = server.server_address
    print(f"L2F review GUI: http://127.0.0.1:{bound_port}/", flush=True)
    if args.host == "0.0.0.0":
        print(f"iPhone/LAN URL: http://{best_lan_ip()}:{bound_port}/", flush=True)
    print(f"Queue: {config.queue_path.relative_to(REPO_ROOT)}", flush=True)
    print(f"Reviews: {config.review_path.relative_to(REPO_ROOT)}", flush=True)
    print("Press Ctrl-C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
