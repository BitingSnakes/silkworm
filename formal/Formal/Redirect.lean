import Formal.Request

namespace Silkworm

def isRedirectStatus (status : Nat) : Bool :=
  status == 301 || status == 302 || status == 303 || status == 307 || status == 308

def redirectChangesToGetStatus (status : Nat) : Bool :=
  status == 301 || status == 302 || status == 303

def isGetOrHead : Method -> Bool
  | Method.GET => true
  | Method.HEAD => true
  | _ => false

def redirectChangesMethodToGet (status : Nat) (method : Method) : Bool :=
  redirectChangesToGetStatus status && !isGetOrHead method

def redirectRequest
    (req : Request)
    (target : Url)
    (status : Nat) : Request :=
  if redirectChangesMethodToGet status req.method then
    { req with
      url := target
      method := Method.GET
      hasBody := false
      hasJson := false
      hasParams := false
      redirects := req.redirects + 1 }
  else
    { req with
      url := target
      hasParams := false
      redirects := req.redirects + 1 }

theorem post_303_redirect_becomes_get
    (req : Request)
    (h : req.method = Method.POST) :
    (redirectRequest req "target" 303).method = Method.GET := by
  simp [redirectRequest, redirectChangesMethodToGet, redirectChangesToGetStatus, isGetOrHead, h]

theorem post_303_redirect_drops_body
    (req : Request)
    (h : req.method = Method.POST) :
    (redirectRequest req "target" 303).hasBody = false := by
  simp [redirectRequest, redirectChangesMethodToGet, redirectChangesToGetStatus, isGetOrHead, h]

theorem post_307_redirect_keeps_method
    (req : Request)
    (h : req.method = Method.POST) :
    (redirectRequest req "target" 307).method = Method.POST := by
  simp [redirectRequest, redirectChangesMethodToGet, redirectChangesToGetStatus, isGetOrHead, h]

theorem redirect_always_drops_params
    (req : Request)
    (target : Url)
    (status : Nat) :
    (redirectRequest req target status).hasParams = false := by
  by_cases h : redirectChangesMethodToGet status req.method
  · simp [redirectRequest, h]
  · simp [redirectRequest, h]

end Silkworm
