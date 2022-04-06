# schedgroup [![Test Status](https://github.com/mdlayher/schedgroup/workflows/Test/badge.svg)](https://github.com/mdlayher/schedgroup/actions) [![Go Reference](https://pkg.go.dev/badge/github.com/mdlayher/schedgroup.svg)](https://pkg.go.dev/github.com/mdlayher/schedgroup) [![Go Report Card](https://goreportcard.com/badge/github.com/mdlayher/schedgroup)](https://goreportcard.com/report/github.com/mdlayher/schedgroup)

Package `schedgroup` provides a goroutine worker pool which schedules tasks
to be performed at or after a specified time. MIT Licensed.

Special thanks to Egon Elbre from #performance on Gophers Slack for [two](https://play.golang.org/p/YyeSWuDil-b)
[prototypes](https://play.golang.org/p/4iYBO6Cgj8m) of this idea, based
on Go's `container/heap` package. Egon's prototypes heavily influenced the final
design of this package.
