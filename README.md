
## 1. Full Architecture Design

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        VELOCITY PROXY (Java 21+)                            │
│                                                                             │
│  ┌──────────────┐    ┌─────────────────────────────────────────────────┐   │
│  │ ChatListener │───▶│           MODERATION PIPELINE                   │   │
│  │ (async event)│    │  Stage1: Local  ──▶  Stage2: Heuristic ──▶      │   │
│  └──────────────┘    │  Stage3: AI Escalation (conditional)            │   │
│                      └──────────────────────────┬──────────────────────┘   │
│                                                 │                           │
│           ┌─────────────────────────────────────▼──────────────────────┐   │
│           │              SERVICE LAYER                                  │   │
│           │  ┌──────────────┐  ┌───────────────┐  ┌─────────────────┐ │   │
│           │  │ AIProvider   │  │ PunishmentEng │  │ AnalyticsEngine │ │   │
│           │  │ (Gemini/GPT/ │  │ + Approval    │  │ + Mood Engine   │ │   │
│           │  │  Claude/LLM) │  │   Workflow    │  │ + Report Gen    │ │   │
│           │  └──────┬───────┘  └───────────────┘  └─────────────────┘ │   │
│           └─────────│──────────────────────────────────────────────────┘   │
│                     │                                                        │
│  ┌──────────────────▼────────────────────────────────────────────────────┐ │
│  │                     INFRASTRUCTURE LAYER                              │ │
│  │  ┌─────────────┐ ┌──────────────┐ ┌────────────┐ ┌────────────────┐ │ │
│  │  │ Caffeine L1 │ │ SemanticCache│ │ CircuitBkr │ │ RateLimiter    │ │ │
│  │  │ (exact/trie)│ │ (AI results) │ │ (per key)  │ │ (token bucket) │ │ │
│  │  └─────────────┘ └──────────────┘ └────────────┘ └────────────────┘ │ │
│  │  ┌─────────────┐ ┌──────────────┐ ┌────────────┐ ┌────────────────┐ │ │
│  │  │ BatchQueue  │ │ KeyRotator   │ │ CostTrackr │ │ MetricsReg     │ │ │
│  │  │ (async AI)  │ │ (multi-key)  │ │ (USD/day)  │ │ (Micrometer)   │ │ │
│  │  └─────────────┘ └──────────────┘ └────────────┘ └────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    DISCORD INTEGRATION                               │   │
│  │  WebhookQueue ──▶ RateSafeClient ──▶ RetryQueue                    │   │
│  │  Channels: moderation | approval | analytics | emergency            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
         │                │               │              │
    [survival]       [skyblock]       [boxpvp]       [prison]  ← Backend servers
```

### Layer Responsibilities

| Layer | Responsibility | Latency Target |
|---|---|---|
| **Listener** | Capture events, dispatch async | < 0.1 ms (non-blocking) |
| **Pipeline Stage 1** | Local exact/trie/regex filters | < 1 ms |
| **Pipeline Stage 2** | Heuristic scoring, context | < 5 ms |
| **Pipeline Stage 3** | AI escalation (conditional) | < 500 ms |
| **Service Layer** | Business logic orchestration | N/A |
| **Infrastructure** | Caching, circuit breaking, metrics | < 0.5 ms |
| **Discord** | Async webhook delivery | background |

---

## 2. Package Structure

```
com.aimoderation
├── AIModerationPlugin.java                   ← Velocity @Plugin entry point
├── bootstrap/
│   ├── PluginBootstrap.java                  ← DI wiring, lifecycle orchestration
│   └── ServiceRegistry.java                  ← Lightweight DI container
│
├── config/
│   ├── loader/
│   │   ├── ConfigLoader.java                 ← Interface
│   │   └── YamlConfigLoader.java             ← SnakeYAML impl, hot-reload
│   ├── model/
│   │   ├── GlobalConfig.java                 ← config.yml
│   │   ├── ApiConfig.java                    ← api.yml (providers, keys, rotation)
│   │   ├── BlacklistConfig.java              ← blacklist.yml (categories, regex)
│   │   ├── PunishmentConfig.java             ← punish.yml
│   │   ├── AnalyticsConfig.java              ← analytics.yml
│   │   └── DiscordConfig.java                ← discord.yml
│   └── watcher/
│       └── ConfigFileWatcher.java            ← WatchService-based hot reload
│
├── api/
│   ├── provider/
│   │   ├── AIProvider.java                   ← Interface (analyze, batchAnalyze)
│   │   ├── AIRequest.java                    ← Value object
│   │   ├── AIResponse.java                   ← Value object
│   │   ├── GeminiProvider.java               ← Gemini REST impl
│   │   ├── OpenAIProvider.java               ← OpenAI REST impl
│   │   └── ClaudeProvider.java               ← Anthropic REST impl
│   ├── rotation/
│   │   ├── KeyRotationStrategy.java          ← Interface
│   │   ├── RoundRobinStrategy.java
│   │   ├── WeightedRotationStrategy.java
│   │   └── AdaptiveRotationStrategy.java     ← Health-score driven
│   ├── health/
│   │   ├── ProviderHealthMonitor.java        ← Per-key health scoring
│   │   └── HealthScore.java                  ← Immutable record
│   ├── circuit/
│   │   ├── CircuitBreaker.java               ← Per-key circuit breaker
│   │   └── CircuitState.java                 ← CLOSED/OPEN/HALF_OPEN
│   ├── ratelimit/
│   │   ├── RateLimiter.java                  ← Interface
│   │   └── TokenBucketRateLimiter.java       ← Per-key token bucket
│   ├── cache/
│   │   ├── AIResponseCache.java              ← Caffeine-backed L2 cache
│   │   └── SemanticCache.java                ← Normalized text → result
│   ├── cost/
│   │   ├── CostTracker.java                  ← Atomic counters, daily reset
│   │   └── DailyCostReport.java              ← Immutable report record
│   ├── batch/
│   │   ├── BatchProcessor.java               ← Collects & fires batched requests
│   │   ├── BatchRequest.java                 ← Pending item with CompletableFuture
│   │   └── BatchWindow.java                  ← Timed window accumulator
│   └── manager/
│       └── AIProviderManager.java            ← Orchestrates providers, rotation, CB
│
├── moderation/
│   ├── pipeline/
│   │   ├── ModerationPipeline.java           ← Orchestrates all stages
│   │   ├── PipelineContext.java              ← Per-message context carrier
│   │   └── PipelineResult.java               ← Final verdict + metadata
│   ├── stage/
│   │   ├── ModerationStage.java              ← Interface (process → StageResult)
│   │   ├── Stage1ExactMatch.java             ← HashMap O(1) lookup
│   │   ├── Stage1TrieLookup.java             ← Trie for prefix detection
│   │   ├── Stage1RegexFilter.java            ← Precompiled regex, Turkish-aware
│   │   ├── Stage1SimilarityCheck.java        ← Levenshtein / soundex
│   │   ├── Stage1CacheLookup.java            ← Previous AI decisions
│   │   ├── Stage2HeuristicScorer.java        ← Score before AI
│   │   └── Stage3AIEscalation.java           ← AI call with dedup + batching
│   ├── filter/
│   │   ├── ExactMatchFilter.java             ← Immutable HashSet<String>
│   │   ├── AhoCorasickTrie.java              ← Multi-pattern trie
│   │   ├── RegexFilterEngine.java            ← Precompiled + categorized
│   │   └── TurkishTextNormalizer.java        ← Unicode normalization, locale
│   ├── context/
│   │   ├── PlayerContextManager.java         ← Manages per-player context
│   │   ├── PlayerContext.java                ← Ring buffer + metadata
│   │   └── MessageRingBuffer.java            ← Fixed-size circular buffer
│   ├── dedup/
│   │   ├── MessageDeduplicator.java          ← Hash → pending future map
│   │   └── NormalizedHasher.java             ← Normalize → hash for dedup
│   ├── result/
│   │   ├── ModerationDecision.java           ← PASS/MUTE/KICK/BAN + reason
│   │   ├── StageResult.java                  ← Per-stage outcome
│   │   └── Severity.java                     ← NONE/LOW/MEDIUM/HIGH/CRITICAL
│   └── learning/
│       ├── CandidateWordLearner.java         ← Stores AI-found candidates
│       └── ApprovalWorkflow.java             ← Manages pending approvals
│
├── punishment/
│   ├── PunishmentEngine.java                 ← Routes decisions to commands
│   ├── PunishmentExecutor.java               ← Executes proxy commands
│   ├── PunishmentTemplate.java               ← {player} {duration} templating
│   └── PunishmentRecord.java                 ← Immutable audit record
│
├── analytics/
│   ├── AnalyticsEngine.java                  ← Per-server analytics orchestrator
│   ├── ServerAnalytics.java                  ← Aggregated server stats
│   ├── TopicExtractor.java                   ← Keyword/topic clustering
│   ├── SentimentAnalyzer.java                ← Positive/negative/neutral
│   ├── BugDetector.java                      ← Bug mention heuristics
│   ├── ExploitDetector.java                  ← Exploit discussion detection
│   ├── MoodEngine.java                       ← Server health score
│   └── report/
│       ├── DailyReportGenerator.java         ← Scheduled daily summaries
│       └── ReportFormatter.java              ← Text/Discord embed formatting
│
├── discord/
│   ├── DiscordWebhookClient.java             ← Async HTTP webhook sender
│   ├── WebhookType.java                      ← MODERATION/APPROVAL/ANALYTICS/EMERGENCY
│   ├── WebhookQueue.java                     ← Bounded async delivery queue
│   ├── RetryQueue.java                       ← Exponential backoff retry
│   ├── embed/
│   │   ├── EmbedBuilder.java                 ← Discord JSON embed construction
│   │   ├── ModerationEmbed.java              ← Mute/ban event embed
│   │   ├── ApprovalEmbed.java                ← Candidate word embed + buttons
│   │   ├── AnalyticsEmbed.java               ← Daily report embed
│   │   └── EmergencyEmbed.java               ← High-severity alert embed
│   └── interaction/
│       ├── InteractionServer.java            ← Lightweight HTTP server for Discord interactions
│       ├── ApprovalHandler.java              ← Approve/reject button logic
│       └── InteractionVerifier.java          ← Ed25519 signature verification
│
├── metrics/
│   ├── MetricsRegistry.java                  ← Micrometer registry
│   ├── ModerationMetrics.java                ← Latency, hits, misses
│   ├── AIMetrics.java                        ← API latency, token counts
│   ├── CacheMetrics.java                     ← Hit/miss rates
│   └── HealthDashboard.java                  ← Aggregated health view
│
├── storage/
│   ├── StorageManager.java                   ← File I/O coordinator
│   ├── AtomicYamlWriter.java                 ← Write-then-rename for atomicity
│   └── BlacklistStorage.java                 ← ai-blacklist-added.yml manager
│
├── command/
│   ├── AIModerationCommand.java              ← /aimod root command
│   └── subcommand/
│       ├── StatusSubcommand.java             ← Live metrics
│       ├── ReloadSubcommand.java             ← Hot config reload
│       ├── ReportSubcommand.java             ← On-demand analytics
│       ├── CostSubcommand.java               ← Token cost report
│       └── ApproveSubcommand.java            ← In-game approval
│
├── listener/
│   ├── ChatListener.java                     ← PlayerChatEvent (async)
│   └── PlayerListener.java                   ← Connect/disconnect tracking
│
├── security/
│   ├── ApiKeyManager.java                    ← Encrypted key storage
│   ├── SecretsMasker.java                    ← Mask keys in logs
│   └── PermissionManager.java                ← Permission node resolver
│
└── util/
    ├── TextNormalizer.java                   ← General text normalization
    ├── TurkishLocaleUtil.java                ← Turkish-specific utilities
    ├── AsyncUtil.java                        ← Virtual thread helpers
    ├── JsonUtil.java                         ← Jackson helpers
    └── TimeUtil.java                         ← Duration formatting
```

---

## 3. Threading Model

### Java 21 Virtual Thread Strategy

```
Event Thread (Netty I/O)
    │
    ▼
ChatListener.onChat()           ← Netty event loop — do NOTHING blocking here
    │  .fireAndForget()
    ▼
VirtualThread-A                 ← Spawned per message (virtual, cheap)
    │  Stage 1 (nanoseconds)    ← Local filters — no I/O, stays on VT
    │  Stage 2 (microseconds)   ← Heuristic — no I/O, stays on VT
    │  Stage 3 decision:
    │      if AI needed ──────▶ BatchQueue (non-blocking offer)
    │                               │
    │                               ▼
    │                        BatchProcessor (VirtualThread-B, scheduled)
    │                           Collects 10–50ms window
    │                           CompletableFuture<AIResponse>
    │                           HTTP call (virtual thread, non-blocking)
    │                           Response fan-out to waiting futures
    │
    ▼ (result arrives via future callback, no thread block)
PunishmentExecutor              ← Proxy command dispatch via Velocity scheduler
    │
    ▼
DiscordWebhookQueue             ← Async producer, VirtualThread consumer
```

### Thread Pools & Executors

| Pool | Type | Size | Purpose |
|---|---|---|---|
| `virtualThreadExecutor` | `newVirtualThreadPerTaskExecutor()` | Unbounded (OS managed) | Per-message pipeline |
| `batchProcessorScheduler` | `ScheduledThreadPool` (1 thread) | 1 | Batch window timer |
| `webhookDelivery` | `newVirtualThreadPerTaskExecutor()` | Unbounded | Discord HTTP |
| `analyticsWorker` | `newVirtualThreadPerTaskExecutor()` | Unbounded | Background analytics |
| `configWatcher` | `SingleThreadScheduled` | 1 | Config file polling |
| `reportScheduler` | `ScheduledThreadPool` (1 thread) | 1 | Daily report trigger |

### Critical Rules
- **Zero `Thread.sleep()`** anywhere in hot path
- **Zero synchronous HTTP** on any event thread
- **All CompletableFuture** callbacks run on `virtualThreadExecutor`
- **All YAML reads/writes** go through `AtomicYamlWriter` on a single-writer thread
- **Caffeine** is already non-blocking internally — use `AsyncLoadingCache` variant

---

## 4. Cache Strategy

### Multi-Level Cache Architecture

```
Message arrives
     │
     ▼
L0: Bloom Filter              ← "Definitely NOT toxic" fast-path (false negative impossible)
     │  MIGHT BE toxic?
     ▼
L1: Exact HashMap             ← O(1), immutable Set<String>, loaded at startup
     │  miss
     ▼
L2: Trie (Aho-Corasick)       ← Multi-pattern substring match, O(n) where n=message length
     │  miss
     ▼
L3: Precompiled Regex Cache   ← Caffeine<String, Pattern> — never recompile same pattern
     │  miss
     ▼
L4: AI Decision Cache         ← Caffeine<normalizedHash, AIResponse>
     │    TTL: 2 hours, maxSize: 50,000 entries
     │  miss
     ▼
L5: Semantic Cache            ← Normalized text similarity → cached result
     │    Handles typos: "amk" == "a m k" == "a.m.k"
     │  miss
     ▼
→ AI API call (expensive)
     │
     ▼ store result in L4 + L5
```

### Cache Sizing (for 5000 players)

| Cache | Max Entries | TTL | Memory Est. |
|---|---|---|---|
| ExactMatch HashSet | 10,000 | Forever | ~2 MB |
| Trie (Aho-Corasick) | All blacklist patterns | Forever | ~5 MB |
| Compiled Regex | 500 patterns | Forever | ~1 MB |
| AI Decision Cache | 50,000 messages | 2 hours | ~20 MB |
| Semantic Cache | 10,000 normalized forms | 1 hour | ~8 MB |
| Player Context (ring buffer) | 5,000 players × 10 msgs | Session | ~15 MB |
| **Total** | — | — | **~51 MB** |

### Caffeine Configuration

```java
Caffeine.newBuilder()
    .maximumSize(50_000)
    .expireAfterWrite(Duration.ofHours(2))
    .recordStats()                    // for metrics
    .softValues()                     // GC-friendly under memory pressure
    .build();
```

### Message Deduplication
- Normalize: lowercase → strip punctuation → collapse whitespace → Turkish char normalization
- Hash: `xxHash64(normalized)` — extremely fast, low collision
- Map: `ConcurrentHashMap<Long, CompletableFuture<AIResponse>>`
- First arrival: creates CompletableFuture, makes API call
- Subsequent arrivals (same hash): join same CompletableFuture — ZERO extra API calls

---

## 5. AI Cost Optimization Strategy

### Token Economy Design

```
Optimization Layer          | Estimated Savings
----------------------------|-----------------
Exact/Trie/Regex local     | ~70–80% of messages never reach AI
AI Decision Cache           | ~10–15% additional savings
Message Deduplication       | ~3–5% (viral phrases)
Batch Requests              | Reduces HTTP overhead, not tokens
Semantic Cache              | ~2–3% (typo variants)
Compressed Prompts          | ~40% token reduction per AI call
──────────────────────────────────────────────
Total savings vs naive      | ~92–95%
```

### Compressed AI Prompt (Ultra-optimized)

```
System: Minecraft chat moderator. Analyze for toxic/harmful content.
Turkish+English. Respond ONLY valid JSON.
Output: {"toxic":bool,"confidence":float,"category":"profanity|racism|threats|spam|clean","candidate_words":["w1"]}

User: [MSG]
```
*(~60 tokens system + 5-30 tokens message = 65-90 tokens total vs 200-400 naive)*

### Batching Strategy
- **Window:** 50ms accumulation window
- **Max batch size:** 20 messages per request
- **Batch prompt:** Single system prompt + numbered messages
- **Response:** JSON array matching indices

```
Batch input (20 msgs, ~400 tokens) vs sequential (20 × 90 = 1800 tokens)
Saving: ~78% token reduction for batched messages
```

### Adaptive AI Escalation Threshold
- Score 0.0–1.0 from heuristic stage
- Score ≥ 0.7 → escalate to AI
- Score < 0.7 → trust local decision (PASS)
- Score ≥ 0.95 → direct punishment, skip AI (obviously toxic from regex)

### Repeated Phrase Learning
- Same normalized phrase flagged 3+ times → cache result permanently (no TTL)
- Candidate word added to in-memory fast lookup
- After staff approval: promoted to L1 exact match cache

---

## 6. Data Flow Diagrams

### Chat Message Flow

```
Player types message
        │
        ▼
[Velocity] PlayerChatEvent (async, on Netty thread)
        │
        ├── event.setCancelled(true)  ← Immediately cancel for processing
        │
        ▼
[VirtualThread] ModerationPipeline.process(message, player, server)
        │
        ├──[Stage 1a] ExactMatchFilter.check(normalized)
        │       MATCH? ──▶ [PunishmentEngine] immediate action
        │       PASS ──▶ continue
        │
        ├──[Stage 1b] AhoCorasickTrie.search(normalized)
        │       MATCH? ──▶ [PunishmentEngine] immediate action
        │       PASS ──▶ continue
        │
        ├──[Stage 1c] RegexFilterEngine.check(text)
        │       MATCH? ──▶ [PunishmentEngine] immediate action
        │       PASS ──▶ continue
        │
        ├──[Stage 1d] SimilarityFilter (Levenshtein, soundex)
        │       MATCH? ──▶ confidence score, may escalate
        │       PASS ──▶ continue
        │
        ├──[Stage 1e] AIDecisionCache.get(normalizedHash)
        │       HIT?  ──▶ use cached decision
        │       MISS  ──▶ continue
        │
        ├──[Stage 2] HeuristicScorer.score(message, playerContext)
        │       score ≥ 0.95 ──▶ direct action
        │       score ≥ 0.70 ──▶ escalate to AI
        │       score < 0.70 ──▶ PASS (no AI call)
        │
        ├──[Stage 3] (conditional)
        │       MessageDeduplicator.deduplicate(hash)
        │           existing future? ──▶ join it (no API call)
        │           new? ──▶ BatchQueue.offer(request)
        │               BatchProcessor fires after window
        │               AIProviderManager.analyze(batch)
        │                   KeyRotator picks healthy key
        │                   CircuitBreaker checks state
        │                   RateLimiter permits?
        │                   HTTP call (async)
        │                   Response → cache L4+L5
        │                   CostTracker.record(tokens)
        │
        ▼
[Decision] ModerationDecision (PASS/WARN/MUTE/KICK/BAN)
        │
        ├── PASS ──▶ [Velocity] resend message to server
        │
        ├── WARN ──▶ send warning message to player (async)
        │
        ├── MUTE/KICK/BAN ──▶ PunishmentExecutor
        │       │  execute command via Velocity scheduler
        │       │
        │       ▼
        │   DiscordWebhookQueue.offer(ModerationEmbed)
        │       async delivery to moderation channel
        │
        └── NEW_CANDIDATE ──▶ CandidateWordLearner
                ApprovalWorkflow.createPendingApproval()
                DiscordWebhookQueue.offer(ApprovalEmbed)
                Wait for Discord button interaction
```

### AI Provider Failover Flow

```
AIProviderManager.selectKey()
        │
        ├── KeyRotator.next()  ← based on strategy (round-robin/weighted/adaptive)
        │
        ├── CircuitBreaker.isAllowed(key)?
        │       OPEN ──▶ skip key, try next
        │
        ├── RateLimiter.tryAcquire(key)?
        │       NO ──▶ skip key, try next
        │
        ├── ProviderHealthMonitor.getScore(key) ≥ threshold?
        │       LOW ──▶ prefer other keys (adaptive mode)
        │
        ├── Make HTTP call
        │       SUCCESS ──▶ CircuitBreaker.recordSuccess(key)
        │                   HealthMonitor.recordSuccess(key)
        │                   CostTracker.record(tokens, USD)
        │
        │       RATE_LIMIT (429) ──▶ CircuitBreaker.recordFailure(key)
        │                            cooldown: 60s
        │                            failover to next key
        │
        │       SERVER_ERROR (5xx) ──▶ CircuitBreaker.recordFailure(key)
        │                              exponential backoff retry
        │                              after 3 failures → OPEN circuit
        │
        └── ALL keys OPEN? ──▶ EmergencyWebhook + graceful degradation
                                (trust heuristic, skip AI temporarily)
```

---

## 7. Config Schemas

### config.yml
```yaml
plugin:
  debug: false
  language: "tr"
  servers:                          # monitored servers
    - survival
    - skyblock
    - boxpvp
    - prison
    - minigame

moderation:
  pipeline:
    heuristic-threshold: 0.70       # score >= this → AI escalation
    auto-action-threshold: 0.95     # score >= this → immediate action
    batch-window-ms: 50             # AI batch accumulation window
    max-batch-size: 20              # max messages per AI batch
    player-context-size: 10         # ring buffer depth per player

  cmi-private-messages: true        # intercept CMI /msg
  cross-server-context: true        # share context across servers

performance:
  virtual-threads: true
  max-concurrent-ai-calls: 50
  queue-capacity: 10000
```

### api.yml
```yaml
provider: GEMINI                    # PRIMARY: GEMINI | OPENAI | CLAUDE

rotation-strategy: ADAPTIVE         # ROUND_ROBIN | WEIGHTED | ADAPTIVE

providers:
  gemini:
    model: "gemini-1.5-flash"       # cheapest Gemini model
    keys:
      - "AIzaSy..."
      - "AIzaSy..."
      - "AIzaSy..."
    weights: [1, 1, 1]              # for WEIGHTED strategy
    rate-limit-rpm: 60              # per key
    circuit-breaker:
      failure-threshold: 3
      reset-timeout-seconds: 60
      half-open-test-count: 1

  openai:
    model: "gpt-4o-mini"
    keys: []
    rate-limit-rpm: 500

  claude:
    model: "claude-haiku-4-5-20251001"
    keys: []
    rate-limit-rpm: 60

health-scoring:
  success-weight: 1.0
  failure-weight: -3.0
  decay-per-minute: 0.1
  minimum-score: 0.0
  maximum-score: 10.0

cost-tracking:
  enabled: true
  daily-budget-usd: 5.00            # alert if exceeded
  report-time: "00:00"              # daily report time (UTC)
```

### blacklist.yml
```yaml
settings:
  turkish-normalization: true
  unicode-normalization: NFC
  word-boundary-mode: UNICODE_WORD_BOUNDARY   # \b equivalent for Turkish

categories:

  severe:
    punishment: PERM_MUTE
    description: "Severely toxic language"
    patterns:
      - "(?iu)(?<![\\p{L}\\p{N}])amk(?![\\p{L}\\p{N}])"
      - "(?iu)(?<![\\p{L}\\p{N}])oç(?![\\p{L}\\p{N}])"
      - "(?iu)(?<![\\p{L}\\p{N}])sik(?![\\p{L}\\p{N}])"

  profanity:
    punishment: TEMP_MUTE_15M
    description: "General profanity"
    patterns:
      - "(?iu)(?<![\\p{L}\\p{N}])aptal(?![\\p{L}\\p{N}])"
      - "(?iu)(?<![\\p{L}\\p{N}])salak(?![\\p{L}\\p{N}])"

  racism:
    punishment: PERM_MUTE
    description: "Hate speech / racism"
    patterns: []

  threats:
    punishment: TEMP_BAN_1D
    description: "Real-life threats"
    patterns: []

  spam:
    punishment: TEMP_MUTE_5M
    description: "Spam / advertisement"
    patterns:
      - "(?i)(discord\\.gg|t\\.me)/[a-z0-9]+"
```

### punish.yml
```yaml
punishments:

  PERM_MUTE:
    commands:
      - "lp user {player} permission set minecraft.command.chat false"
      - "alert {player} Permanent mute: {reason}"

  TEMP_MUTE_5M:
    commands:
      - "mute {player} 5m {reason}"

  TEMP_MUTE_15M:
    commands:
      - "mute {player} 15m {reason}"

  TEMP_MUTE_1H:
    commands:
      - "mute {player} 1h {reason}"

  TEMP_MUTE_1D:
    commands:
      - "mute {player} 1d {reason}"

  TEMP_BAN_1D:
    commands:
      - "ban {player} 1d {reason}"

  PERM_BAN:
    commands:
      - "ban {player} {reason}"

  WARN:
    commands:
      - "send {player} &c[UYARI] {reason}"
```

### discord.yml
```yaml
enabled: true

webhooks:
  moderation:
    url: "https://discord.com/api/webhooks/..."
    username: "🛡 ModBot"
    avatar-url: ""

  approval:
    url: "https://discord.com/api/webhooks/..."
    username: "🤖 AI Approval"
    avatar-url: ""

  analytics:
    url: "https://discord.com/api/webhooks/..."
    username: "📊 Analytics"
    avatar-url: ""

  emergency:
    url: "https://discord.com/api/webhooks/..."
    username: "🚨 Emergency"
    avatar-url: ""

interaction-server:
  enabled: true                      # for approval buttons
  port: 8080
  public-key: "..."                  # Discord app public key

rate-limit:
  messages-per-second: 5
  burst: 10
  retry-delay-ms: 500
  max-retries: 3
```

### analytics.yml
```yaml
enabled: true

per-server: true

report:
  schedule: "0 0 * * *"            # daily midnight UTC
  top-topics-count: 10
  top-complaints-count: 5

bug-detection:
  keywords:
    - "bug"
    - "bozuk"
    - "çalışmıyor"
    - "dupe"
    - "hata"
    - "glitch"
  severity-threshold: 3            # mentions before flagging

exploit-detection:
  keywords:
    - "dupe"
    - "bypass"
    - "hack"
    - "cheat"
    - "exploit"
  cluster-window-minutes: 10       # time window for cluster detection
  cluster-threshold: 5             # messages from different players

sentiment:
  window-minutes: 60               # rolling sentiment window
  negative-alert-threshold: 0.60   # alert if >60% negative

mood-scoring:
  update-interval-minutes: 5
  weights:
    toxicity: -2.0
    bug-reports: -0.5
    complaints: -1.0
    positive-sentiment: +1.0
```

---

## 8. Discord Interaction Design

### Approval Flow (Sequence)

```
AI detects new candidate word "xyz"
        │
        ▼
CandidateWordLearner.addCandidate("xyz", confidence=0.89, context="...")
        │
        ▼
ApprovalWorkflow.createPendingApproval(candidate)
        │  stores in memory map: pendingId → CandidateApproval
        │
        ▼
DiscordWebhookQueue.offer(ApprovalEmbed)
        │
        ▼
Approval channel receives:
┌───────────────────────────────────────────────────┐
│ 🤖 AI Candidate Word Detection                    │
├───────────────────────────────────────────────────┤
│ Word: **xyz**                                     │
│ Confidence: 89%                                   │
│ Category suggestion: profanity                    │
│ Detected in: [survival] player: Steve             │
│ Context: "...bu xyz oyun..."                      │
│ Occurrences today: 3                              │
│                                                   │
│  [✅ APPROVE]        [❌ REJECT]                  │
└───────────────────────────────────────────────────┘
        │
    [Button click triggers HTTP POST to InteractionServer]
        │
        ├── APPROVE ──▶ ApprovalHandler.approve(candidateId)
        │                   BlacklistStorage.addWord("xyz", category)
        │                   ExactMatchFilter.addRuntime("xyz")   ← live update
        │                   DiscordWebhookQueue.offer(confirmEmbed)
        │
        └── REJECT  ──▶ ApprovalHandler.reject(candidateId)
                            CandidateWordLearner.addToIgnore("xyz")
                            DiscordWebhookQueue.offer(rejectedEmbed)
```

### Embed Types

| Type | Color | Fields | Frequency |
|---|---|---|---|
| Moderation | 🔴 Red | Player, server, message, action, duration | Per punishment |
| Approval | 🟡 Yellow | Word, confidence, category, context, count | Per new candidate |
| Analytics | 🔵 Blue | Server, top topics, sentiment, mood score | Daily |
| Emergency | 🚨 Red flash | Alert type, severity, details | Threshold breach |

### Webhook Rate Safety
- Token bucket: 5 msg/s, burst 10
- On throttle: queue with exponential backoff (500ms → 1s → 2s → 4s)
- Max queue depth: 1000 items
- On overflow: drop LOW priority, keep HIGH/EMERGENCY
- Retry max: 3 attempts per message

---

## 9. Performance Analysis

### Throughput Projections

| Scenario | Msg/sec | AI calls/sec | Latency P99 |
|---|---|---|---|
| 2000 players, normal | ~50 | ~0.5 (98% local) | < 2ms (local) / < 600ms (AI) |
| 5000 players, peak | ~200 | ~2 (99% local) | < 2ms (local) / < 800ms (AI) |
| Spam attack | ~2000 | ~0 (dedup) | < 5ms (dedup path) |
| Viral toxic phrase | ~500 | 1 (deduplicated) | < 1ms (2nd+ hit) |

### Memory Footprint

| Component | Memory |
|---|---|
| Cache system | ~51 MB |
| Trie (Aho-Corasick) | ~5 MB |
| Thread overhead (5000 VTs) | ~50 MB (VTs are ~300 bytes each) |
| Analytics state | ~10 MB |
| JVM baseline | ~100 MB |
| **Total estimated** | **~220 MB** |

### CPU Profile (expected)
- **Hot path (Stage 1):** ~95% of CPU in regex/trie operations
- **Stage 3 (AI):** Async, non-blocking → near-zero CPU while waiting
- **Background tasks:** < 1% CPU for analytics, reporting
- **GC:** G1GC recommended, expected < 5ms pause times at this heap size

### Benchmark Expectations
- `ExactMatchFilter.check()`: < 100 ns
- `AhoCorasickTrie.search()`: < 1 µs (for 10k patterns, 100 char message)
- `RegexFilterEngine.check()` (precompiled): < 500 µs
- `SemanticCache.get()`: < 10 µs
- `TurkishTextNormalizer.normalize()`: < 50 µs
- `ModerationPipeline.process()` (local path): < 2 ms P99
- `AIProvider.analyze()` (Gemini 1.5 Flash): 300–800 ms P99
- `DiscordWebhookClient.send()`: background, non-blocking

---

## 10. Scalability Justification

### Horizontal Scalability
While a single Velocity proxy can typically handle 5000+ players, the design supports multi-proxy setups:

- **Shared state:** Redis-compatible interface (optional) for cross-proxy cache sharing
- **Stateless pipeline:** Each proxy can run independent pipeline
- **Event bus:** If shared state needed, Redis pub/sub for moderation events
- **Analytics aggregation:** Each proxy writes to shared DB/file, central aggregator produces reports

### Vertical Scalability
- Virtual threads allow thousands of concurrent in-flight messages without thread pool exhaustion
- Caffeine caches scale up to JVM heap limits (configurable)
- Batch processor adapts window size dynamically based on load

### Single Proxy (Primary Design)
- Velocity handles 5000 players on a single proxy efficiently
- Plugin overhead: ~220 MB RAM, < 5% extra CPU
- No external dependencies required (Redis, DB optional)

---

## 11. Failure Handling Strategy

### Circuit Breaker States

```
CLOSED ──(failures >= threshold)──▶ OPEN ──(timeout elapsed)──▶ HALF_OPEN
   ▲                                                                  │
   └─────────────(probe success)──────────────────────────────────────┘
                                   │ probe fails
                                   ▼
                                 OPEN (reset timer)
```

### AI Provider Failure Scenarios

| Failure | Response | Recovery |
|---|---|---|
| Rate limit (429) | Skip key, cooldown 60s | Automatic after cooldown |
| Server error (5xx) | Retry with backoff (1s,2s,4s) | After 3 fails → circuit open |
| Timeout | Cancel future, try next key | Immediate |
| All keys exhausted | Log emergency, use heuristic only | Alert via Discord emergency webhook |
| API shutdown | Graceful degradation, trust local filters | Manual intervention |

### Configuration Failure
- Bad YAML → reject new config, keep running with last good config
- Missing keys → use defaults with warning logged
- Atomic file writes → power-failure safe (write tmp → rename)

### Queue Overflow
- Bounded queues with `offer()` (never `put()` — never block)
- On full: log metric, drop lowest priority item
- Backpressure via async flow control

### Discord Webhook Failure
- 3 retries with exponential backoff
- After 3 fails: log locally, continue moderation (Discord is not critical path)
- Emergency channel failure: duplicate to secondary webhook

---

## 12. Security Review

### API Key Management
- Keys never logged (SecretsMasker replaces with `***REDACTED***`)
- Keys stored in api.yml with file permissions `600` (owner read only)
- In-memory: stored as `char[]` not `String` (for GC clearability)
- Keys masked in all exception messages, stack traces

### Discord Interaction Security
- Ed25519 signature verification on all interaction POSTs
- Reject any interaction without valid `X-Signature-Ed25519` header
- Timing-safe comparison for signatures (prevent timing attacks)
- Rate limit interaction endpoint: 100 req/s max

### Permission System
- `aimod.admin` → all commands
- `aimod.status` → view metrics
- `aimod.reload` → config reload
- `aimod.approve` → in-game approval
- `aimod.report` → trigger reports

### File Security
- Config writes: tmp file → `Files.move(ATOMIC_MOVE)` — never partial write
- Webhook URLs in config: warn if committed to version control
- No player data stored permanently (analytics are aggregated, not per-player PII)

### Input Validation
- Message length cap before processing (prevent regex DoS on huge strings)
- Regex complexity limits (prevent ReDoS — use possessive quantifiers, atomic groups)
- AI response JSON validation (never trust raw AI output — parse defensively)

---

## 13. Benchmark Expectations

### JMH Targets (to be verified in CI)

| Benchmark | Target | Notes |
|---|---|---|
| `ExactMatchFilter.check` | < 100 ns/op | HashSet lookup |
| `AhoCorasickTrie.search` | < 1000 ns/op | For 10k patterns |
| `TurkishTextNormalizer.normalize` | < 50,000 ns/op | Unicode normalize |
| `RegexFilterEngine.check` (10 patterns) | < 500,000 ns/op | Precompiled |
| `SemanticCache.get` (hit) | < 1000 ns/op | Caffeine get |
| `MessageDeduplicator.deduplicate` | < 2000 ns/op | ConcurrentHashMap |
| `ModerationPipeline.process` (local) | < 2 ms | Full Stage 1+2 |
| `HeuristicScorer.score` | < 100,000 ns/op | No I/O |
| `BatchProcessor.offer` | < 5000 ns/op | Queue offer |

### Load Test Targets
- 5000 concurrent players, 50 msg/s/player burst → 250,000 msg/s sustained
- P99 local filter latency: < 5 ms
- P99 AI-escalated latency: < 1000 ms
- Memory stable over 24h under load (no leaks)
- GC pause P99: < 10 ms (G1GC)

---

## 14. Risk Analysis

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| AI API rate limit during attack | HIGH | MEDIUM | Multi-key rotation + dedup makes this a non-issue |
| Regex ReDoS attack | MEDIUM | HIGH | Atomic groups, input length limits, timeout per regex |
| False positive mutes | MEDIUM | HIGH | Confidence thresholds + approval workflow |
| Memory leak (cache growth) | LOW | HIGH | Caffeine soft refs + max size + monitoring |
| Discord webhook spam | LOW | LOW | Rate limiter + queue overflow protection |
| Config corruption on crash | LOW | HIGH | Atomic file writes + backup last-good config |
| AI provider shutdown | LOW | MEDIUM | Multi-provider abstraction, fallback to local |
| Turkish charset false positives | MEDIUM | MEDIUM | Unicode word boundary patterns, extensive testing |
| Virtual thread starvation | LOW | HIGH | Never block on carriers, all I/O async |
| Cost overrun (AI tokens) | LOW | MEDIUM | Daily budget cap + emergency alert |
| Approval button spam | LOW | MEDIUM | Per-interaction idempotency key |

---
