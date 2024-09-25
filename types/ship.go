package types

type ContractRow struct {
	Code       string `json:"code"`
	Scope      string `json:"scope"`
	Table      string `json:"table"`
	PrimaryKey string `json:"primary_key"`
	Value      []byte `json:"value"`
}
