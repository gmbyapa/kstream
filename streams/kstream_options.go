package streams

//type StreamOption func(*kStreamOptions)
//
//func StreamWithOffset(offset consumer.Offset) StreamOption {
//	return func(options *kStreamOptions) {
//		options.consumer.offset.Reset = offset
//	}
//}
//
//type kStreamOptions struct {
//	consumer struct{
//		offset struct{
//			Reset consumer.Offset
//		}
//	}
//	recordContextExtractFunc consumer.RecordContextExtractFunc
//}
//
//func (opts *kStreamOptions) applyDefault()  {
//	opts.consumer.offset.Reset = consumer.Latest
//}
