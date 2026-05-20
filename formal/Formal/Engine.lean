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

def AllPriorityLe (priority : Int) : List Request -> Prop
  | [] => True
  | req :: rest => priority >= req.priority ∧ AllPriorityLe priority rest

/-- The engine queue is ordered from highest priority to lowest priority. -/
def PrioritySorted : List Request -> Prop
  | [] => True
  | req :: rest => AllPriorityLe req.priority rest ∧ PrioritySorted rest

theorem allPriorityLe_trans
    {higher lower : Int}
    (hPriority : higher >= lower) :
    ∀ {queue : List Request},
      AllPriorityLe lower queue ->
      AllPriorityLe higher queue
  | [], _ => trivial
  | _ :: _, hQueue =>
      ⟨Int.le_trans hQueue.1 hPriority, allPriorityLe_trans hPriority hQueue.2⟩

theorem allPriorityLe_enqueueByPriority
    (priority : Int)
    (req : Request)
    (hReq : priority >= req.priority) :
    ∀ {queue : List Request},
      AllPriorityLe priority queue ->
      AllPriorityLe priority (enqueueByPriority req queue)
  | [], _ => by
      simp [enqueueByPriority, AllPriorityLe]
      exact hReq
  | queued :: rest, hQueue => by
      by_cases hBefore : req.priority > queued.priority
      · simp [enqueueByPriority, AllPriorityLe, hBefore]
        exact ⟨hReq, hQueue⟩
      · simp [enqueueByPriority, AllPriorityLe, hBefore] at hQueue ⊢
        exact ⟨hQueue.1, allPriorityLe_enqueueByPriority priority req hReq hQueue.2⟩

theorem enqueueByPriority_preserves_prioritySorted
    (req : Request) :
    ∀ {queue : List Request},
      PrioritySorted queue ->
      PrioritySorted (enqueueByPriority req queue)
  | [], _ => by
      simp [enqueueByPriority, PrioritySorted, AllPriorityLe]
  | queued :: rest, hSorted => by
      by_cases hBefore : req.priority > queued.priority
      · simp [enqueueByPriority, PrioritySorted, AllPriorityLe, hBefore] at hSorted ⊢
        exact
          ⟨⟨Int.le_of_lt hBefore,
              allPriorityLe_trans (Int.le_of_lt hBefore) hSorted.1⟩,
            hSorted⟩
      · simp [enqueueByPriority, PrioritySorted, hBefore] at hSorted ⊢
        exact
          ⟨allPriorityLe_enqueueByPriority queued.priority req (Int.le_of_not_gt hBefore) hSorted.1,
            enqueueByPriority_preserves_prioritySorted req hSorted.2⟩

theorem enqueueByPriority_stays_after_higher_or_equal
    (req queued : Request)
    (rest : List Request)
    (hNotBefore : ¬ req.priority > queued.priority) :
    enqueueByPriority req (queued :: rest) =
      queued :: enqueueByPriority req rest := by
  simp [enqueueByPriority, hNotBefore]

theorem enqueueByPriority_equal_priority_fifo
    (req queued : Request)
    (rest : List Request)
    (hPriority : req.priority = queued.priority) :
    enqueueByPriority req (queued :: rest) =
      queued :: enqueueByPriority req rest := by
  apply enqueueByPriority_stays_after_higher_or_equal
  simp [hPriority]

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

theorem enqueueWith_preserves_prioritySorted
    (key : DedupKey)
    (req : Request)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (enqueueWith key req st).queue := by
  by_cases hFilter : req.dontFilter = true
  · simp [enqueueWith, hFilter]
    exact enqueueByPriority_preserves_prioritySorted req hSorted
  · by_cases hSeen : key req ∈ st.seen
    · simp [enqueueWith, hFilter, hSeen, hSorted]
    · simp [enqueueWith, hFilter, hSeen]
      exact enqueueByPriority_preserves_prioritySorted req hSorted

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

theorem scrapeItem_increments_itemsScraped
    (item : Item)
    (st : EngineState) :
    (scrapeItem item st).stats.itemsScraped = st.stats.itemsScraped + 1 := by
  simp [scrapeItem]

theorem scrapeItem_preserves_queue
    (item : Item)
    (st : EngineState) :
    (scrapeItem item st).queue = st.queue := by
  simp [scrapeItem]

def fetchSuccess (_response : Response) (st : EngineState) : EngineState :=
  { st with
    stats := {
      st.stats with
      requestsSent := st.stats.requestsSent + 1
      responsesReceived := st.stats.responsesReceived + 1
    } }

def fetchError (_req : Request) (st : EngineState) : EngineState :=
  { st with
    stats := {
      st.stats with
      requestsSent := st.stats.requestsSent + 1
      errors := st.stats.errors + 1
    } }

theorem fetchSuccess_increments_requestsSent
    (response : Response)
    (st : EngineState) :
    (fetchSuccess response st).stats.requestsSent = st.stats.requestsSent + 1 := by
  simp [fetchSuccess]

theorem fetchSuccess_increments_responsesReceived
    (response : Response)
    (st : EngineState) :
    (fetchSuccess response st).stats.responsesReceived =
      st.stats.responsesReceived + 1 := by
  simp [fetchSuccess]

theorem fetchSuccess_preserves_queue
    (response : Response)
    (st : EngineState) :
    (fetchSuccess response st).queue = st.queue := by
  simp [fetchSuccess]

theorem fetchError_increments_requestsSent
    (req : Request)
    (st : EngineState) :
    (fetchError req st).stats.requestsSent = st.stats.requestsSent + 1 := by
  simp [fetchError]

theorem fetchError_increments_errors
    (req : Request)
    (st : EngineState) :
    (fetchError req st).stats.errors = st.stats.errors + 1 := by
  simp [fetchError]

theorem fetchError_preserves_queue
    (req : Request)
    (st : EngineState) :
    (fetchError req st).queue = st.queue := by
  simp [fetchError]

def handleEventWith (key : DedupKey) (event : Event) (st : EngineState) : EngineState :=
  match event with
  | Event.request req => enqueueWith key req st
  | Event.item item => scrapeItem item st

def handleEvent (event : Event) (st : EngineState) : EngineState :=
  handleEventWith defaultDedupKey event st

def handleEventsWith (key : DedupKey) (events : List Event) (st : EngineState) : EngineState :=
  events.foldl (fun acc event => handleEventWith key event acc) st

def handleEvents (events : List Event) (st : EngineState) : EngineState :=
  handleEventsWith defaultDedupKey events st

def handleCallbackOutputWith
    (key : DedupKey)
    (out : CallbackOutput)
    (st : EngineState) : EngineState :=
  handleEventsWith key (normalizeCallbackOutput out) st

def handleCallbackOutput
    (out : CallbackOutput)
    (st : EngineState) : EngineState :=
  handleCallbackOutputWith defaultDedupKey out st

theorem handleEventWith_preserves_prioritySorted
    (key : DedupKey)
    (event : Event)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (handleEventWith key event st).queue := by
  cases event with
  | request req =>
      simp [handleEventWith]
      exact enqueueWith_preserves_prioritySorted key req st hSorted
  | item item =>
      simp [handleEventWith, scrapeItem, hSorted]

theorem handleEventsWith_preserves_prioritySorted
    (key : DedupKey)
    (events : List Event)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (handleEventsWith key events st).queue := by
  induction events generalizing st with
  | nil =>
      simpa [handleEventsWith] using hSorted
  | cons event events ih =>
      simp [handleEventsWith]
      exact ih (handleEventWith key event st)
        (handleEventWith_preserves_prioritySorted key event st hSorted)

theorem handleCallbackOutputWith_preserves_prioritySorted
    (key : DedupKey)
    (out : CallbackOutput)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (handleCallbackOutputWith key out st).queue := by
  exact
    handleEventsWith_preserves_prioritySorted key
      (normalizeCallbackOutput out)
      st
      hSorted

end Silkworm
