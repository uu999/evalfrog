// Package scheduling owns project-fair admission, lanes, credits, and bounded dispatch.
// PostgreSQL remains authoritative; every Redis structure in this package can
// be discarded and rebuilt from runtime facts.
package scheduling
