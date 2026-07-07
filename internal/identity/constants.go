package identity

// Identity kinds (identity.identities.kind).
const (
	KindUser           = "user"
	KindGroup          = "group"
	KindServiceAccount = "service_account"
	KindWorkflow       = "workflow"
)

// Identity sources (identity.identities.source).
const (
	SourceLocal        = "local"        // API/bootstrap-created
	SourceExternal     = "external"     // IdentityMapping CR
	SourceExternalAuto = "external-auto" // open-mode auto-provision (HOR-313, deferred)
)
