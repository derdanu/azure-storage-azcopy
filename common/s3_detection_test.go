package common

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3URLDetection(t *testing.T) {
	a := assert.New(t)
	cases := []struct{
		name string
		urlStr string
		wantIsS3 bool
		wantParse bool
	}{
		{"AWS vhost", "http://bucket.s3.amazonaws.com", true, true},
		{"AWS region vhost", "http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname", true, true},
		{"dualstack", "http://bucket.s3.dualstack.aws-region.amazonaws.com/keyname/", true, true},
		{"AWS service", "https://s3.amazonaws.com", true, true},
		
		// HTTP with various port combinations
		{"http default port 80", "http://bucket.example.com:80/object", true, true},
		{"http custom port", "http://bucket.example.com:8080/object", true, true},
		{"http high port", "http://bucket.example.com:9000/object", true, true},
		{"http path-style port", "http://s3.example.com:9000/bucket/object", true, true},
		{"http vhost-style port", "http://bucket.s3.example.com:8080/object", true, true},
		
		// HTTPS with various port combinations
		{"https default port 443", "https://bucket.example.com:443/object", true, true},
		{"https custom port", "https://bucket.example.com:8443/object", true, true},
		{"https high port", "https://bucket.example.com:9443/object", true, true},
		{"https path-style port", "https://s3.example.com:9443/bucket/object", true, true},
		{"https vhost-style port", "https://bucket.s3.example.com:8443/object", true, true},
		
		// IP addresses with ports
		{"http IPv4 port", "http://192.168.1.100:9000/bucket/object", true, true},
		{"https IPv4 port", "https://192.168.1.100:9443/bucket/object", true, true},
		{"http localhost port", "http://localhost:9000/bucket/object", true, true},
		{"https localhost port", "https://localhost:9443/bucket/object", true, true},
		{"http IPv6 port", "http://[::1]:9000/bucket/object", true, true},
		{"https IPv6 port", "https://[::1]:9443/bucket/object", true, true},
		
		// MinIO examples with ports
		{"MinIO host port vhost", "http://bucket.minio.local:9000/object", true, true},
		{"MinIO path-style", "http://minio.local:9000/bucket/object", true, true},
		{"MinIO https vhost", "https://bucket.minio.local:9443/object", true, true},
		{"MinIO https path", "https://minio.local:9443/bucket/object", true, true},
		
		// Custom FQDN examples with HTTP and HTTPS
		{"custom FQDN http vhost", "http://bucket.storage.company.com/object", true, true},
		{"custom FQDN https vhost", "https://bucket.storage.company.com/object", true, true},
		{"custom FQDN http path", "http://storage.company.com/bucket/object", true, true},
		{"custom FQDN https path", "https://storage.company.com/bucket/object", true, true},
		{"custom FQDN http port vhost", "http://bucket.s3.internal.corp:8080/object", true, true},
		{"custom FQDN https port vhost", "https://bucket.s3.internal.corp:8443/object", true, true},
		{"custom FQDN http port path", "http://s3.internal.corp:8080/bucket/object", true, true},
		{"custom FQDN https port path", "https://s3.internal.corp:8443/bucket/object", true, true},
		{"subdomain http", "http://bucket.s3.region.example.org/object", true, true},
		{"subdomain https", "https://bucket.s3.region.example.org/object", true, true},
		{"deep subdomain http", "http://bucket.storage.region.datacenter.company.net/object", true, true},
		{"deep subdomain https", "https://bucket.storage.region.datacenter.company.net/object", true, true},
		{"custom TLD http", "http://bucket.s3.local/object", true, true},
		{"custom TLD https", "https://bucket.s3.local/object", true, true},
		
		// s3 scheme
		{"s3 scheme", "s3://bucket/object", true, true},
		
		// Negative cases
		{"ftp scheme", "ftp://bucket.s3.amazonaws.com", false, false},
		{"azure blob like", "http://s3-test.blob.core.windows.net", false, false},
		{"empty host", "http:///bucket", false, false},
		{"smtp scheme port", "smtp://invalid.com:587", false, false},
		{"invalid scheme port", "ldap://server.com:389/bucket", false, false},
	}

	for _, c := range cases {
		u, err := url.Parse(c.urlStr)
		a.NoError(err, c.name+": parse")
		is := IsS3URL(*u)
		a.Equal(c.wantIsS3, is, c.name+": IsS3URL")
		_, err = NewS3URLParts(*u)
		a.Equal(c.wantParse, err == nil, c.name+": NewS3URLParts")
	}
}
