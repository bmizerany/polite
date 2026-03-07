package polite

import "context"

func ExamplePool_Get() {
	ctx := context.Background()

	p := &Pool{Open: Opener(DefaultDSN, nil)}
	defer p.Close()

	cn, err := p.Get(ctx)
	if err != nil {
		return
	}
	defer p.Put(cn)

	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		return
	}
}
