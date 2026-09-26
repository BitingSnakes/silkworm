import Silkworm.Callback
import Silkworm.Engine

namespace Silkworm

theorem handle_no_callback_effects_noop (st : EngineState) :
    handleCallbackEffects [] st = st := by
  simp [
    handleCallbackEffects,
    handleCallbackEffectsWith,
    handleEventsWith,
  ]

end Silkworm
