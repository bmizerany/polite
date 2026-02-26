// Package pqlite is a small PostgreSQL client that works directly at the
// protocol level, without database/sql.
//
// [Conn] represents a single backend session for executing queries and COPY
// operations. [Pool] shares connections across goroutines. [Listener] provides
// a dedicated LISTEN/NOTIFY connection.
//
// Query-style APIs stream results as iter.Seq2 values and are consumed with
// range:
//
//	for row, err := range cn.Query(ctx, "SELECT ...") {
//		if err != nil {
//			// handle error
//			break
//		}
//		// read row values
//	}
package pqlite
