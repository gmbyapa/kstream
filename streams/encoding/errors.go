package encoding

type Err struct {
	Err error
}

func (err Err) Error() string {
	return err.Err.Error()
}
