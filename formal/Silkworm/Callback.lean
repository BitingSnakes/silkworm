import Silkworm.Engine

namespace Silkworm

/-! Callbacks report results through `emit`/`follow` effects (`CallbackEffects`). -/

theorem handleCallbackEffectsWith_nil (key : DedupKey) (st : EngineState) :
    handleCallbackEffectsWith key [] st = st := by
  simp [handleCallbackEffectsWith, handleEventsWith]

/-- `await emit(item)` hands exactly that item to the pipelines. -/
theorem handleCallbackEffectsWith_emit
    (key : DedupKey)
    (item : Item)
    (st : EngineState) :
    handleCallbackEffectsWith key [Event.item item] st = scrapeItem item st := by
  simp [handleCallbackEffectsWith, handleEventsWith, handleEventWith]

/-- `await follow(request)` enqueues exactly that request. -/
theorem handleCallbackEffectsWith_follow
    (key : DedupKey)
    (req : Request)
    (st : EngineState) :
    handleCallbackEffectsWith key [Event.request req] st = enqueueWith key req st := by
  simp [handleCallbackEffectsWith, handleEventsWith, handleEventWith]

/--
  Effects apply in call order: running a callback whose trace is `xs ++ ys`
  equals applying `xs` and then `ys`, so each awaited effect is observed by the
  engine before the callback continues.
-/
theorem handleCallbackEffectsWith_append
    (key : DedupKey)
    (xs ys : CallbackEffects)
    (st : EngineState) :
    handleCallbackEffectsWith key (xs ++ ys) st =
      handleCallbackEffectsWith key ys (handleCallbackEffectsWith key xs st) := by
  simp [handleCallbackEffectsWith, handleEventsWith, List.foldl_append]

end Silkworm
