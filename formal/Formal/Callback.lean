import Formal.Request

namespace Silkworm

theorem normalize_none_empty :
    normalizeCallbackOutput CallbackOutput.none = [] := by
  simp [normalizeCallbackOutput]

theorem normalize_one_singleton (event : Event) :
    normalizeCallbackOutput (CallbackOutput.one event) = [event] := by
  simp [normalizeCallbackOutput]

theorem normalize_many_identity (events : List Event) :
    normalizeCallbackOutput (CallbackOutput.many events) = events := by
  simp [normalizeCallbackOutput]

end Silkworm
