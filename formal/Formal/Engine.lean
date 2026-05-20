import Formal.Request

namespace Silkworm

structure EngineStats where
  requestsSent : Nat := 0
  responsesReceived : Nat := 0
  itemsScraped : Nat := 0
  errors : Nat := 0
deriving Repr, DecidableEq

structure EngineState where
  seen : List Url := []
  queue : List Request := []
  scraped : List Item := []
  stats : EngineStats := {}
deriving Repr, DecidableEq

abbrev DedupKey := Request -> Url

/-- Default silkworm deduplication keys only on `Request.url`. -/
def defaultDedupKey : DedupKey :=
  fun req => req.url

def dedupKey (req : Request) : Url :=
  req.url

def enqueueByPriority (req : Request) : List Request -> List Request
  | [] => [req]
  | queued :: rest =>
      if req.priority > queued.priority then
        req :: queued :: rest
      else
        queued :: enqueueByPriority req rest

/-!
  Enqueue semantics:
  - `dontFilter = true` always appends the request;
  - higher priority requests are placed before lower priority requests;
  - equal priority requests keep FIFO order;
  - a seen deduplication key with filtering enabled leaves the state unchanged;
  - an unseen deduplication key with filtering enabled is marked seen and inserted.
-/
def enqueueWith (key : DedupKey) (req : Request) (st : EngineState) : EngineState :=
  if req.dontFilter then
    { st with queue := enqueueByPriority req st.queue }
  else if key req ∈ st.seen then
    st
  else
    { st with
      seen := key req :: st.seen
      queue := enqueueByPriority req st.queue }

/-- Enqueue using the default URL-only deduplication key. -/
def enqueue : Request -> EngineState -> EngineState :=
  enqueueWith defaultDedupKey

theorem enqueueWith_unseen_marks_seen
    (key : DedupKey)
    (req : Request)
    (st : EngineState)
    (hFilter : req.dontFilter = false)
    (hUnseen : key req ∉ st.seen) :
    key req ∈ (enqueueWith key req st).seen := by
  simp [enqueueWith, hFilter, hUnseen]

theorem enqueueWith_seen_is_noop
    (key : DedupKey)
    (req : Request)
    (st : EngineState)
    (hFilter : req.dontFilter = false)
    (hSeen : key req ∈ st.seen) :
    enqueueWith key req st = st := by
  simp [enqueueWith, hFilter, hSeen]

theorem enqueueWith_dontFilter_preserves_seen
    (key : DedupKey)
    (req : Request)
    (st : EngineState)
    (h : req.dontFilter = true) :
    (enqueueWith key req st).seen = st.seen := by
  simp [enqueueWith, h]

theorem enqueue_unseen_marks_seen
    (req : Request)
    (st : EngineState)
    (hFilter : req.dontFilter = false)
    (hUnseen : dedupKey req ∉ st.seen) :
    dedupKey req ∈ (enqueue req st).seen := by
  exact enqueueWith_unseen_marks_seen defaultDedupKey req st hFilter hUnseen

theorem enqueue_seen_is_noop
    (req : Request)
    (st : EngineState)
    (hFilter : req.dontFilter = false)
    (hSeen : dedupKey req ∈ st.seen) :
    enqueue req st = st := by
  exact enqueueWith_seen_is_noop defaultDedupKey req st hFilter hSeen

theorem enqueue_dontFilter_preserves_seen
    (req : Request)
    (st : EngineState)
    (h : req.dontFilter = true) :
    (enqueue req st).seen = st.seen := by
  exact enqueueWith_dontFilter_preserves_seen defaultDedupKey req st h

def scrapeItem (item : Item) (st : EngineState) : EngineState :=
  { st with
    scraped := st.scraped ++ [item]
    stats := { st.stats with itemsScraped := st.stats.itemsScraped + 1 } }

def handleEvent (event : Event) (st : EngineState) : EngineState :=
  match event with
  | Event.request req => enqueue req st
  | Event.item item => scrapeItem item st

def handleEvents (events : List Event) (st : EngineState) : EngineState :=
  events.foldl (fun acc event => handleEvent event acc) st

def handleCallbackOutput
    (out : CallbackOutput)
    (st : EngineState) : EngineState :=
  handleEvents (normalizeCallbackOutput out) st

end Silkworm
