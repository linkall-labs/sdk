// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vanus

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	stdtime "time"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maximumNumberPerGetRequest = 64
	ContentTypeProtobuf        = "application/protobuf"
	httpRequestPrefix          = "/gatewaysink"
	datacontenttype            = "datacontenttype"
	dataschema                 = "dataschema"
	subject                    = "subject"
	timeAttr                   = "time"
)

var (
	emptyID = uint64(0)
	base    = 16
	bitSize = 64
)

var (
	zeroTime   = stdtime.Time{}
	ErrEmptyID = errors.New("id: empty")
)

func ToProto(e *v2.Event) (*cloudevents.CloudEvent, error) {
	container := &cloudevents.CloudEvent{
		Id:          e.ID(),
		Source:      e.Source(),
		SpecVersion: e.SpecVersion(),
		Type:        e.Type(),
		Attributes:  make(map[string]*cloudevents.CloudEvent_CloudEventAttributeValue),
	}
	if e.DataContentType() != "" {
		container.Attributes[datacontenttype], _ = attributeFor(e.DataContentType())
	}
	if e.DataSchema() != "" {
		container.Attributes[dataschema], _ = attributeFor(e.DataSchema())
	}
	if e.Subject() != "" {
		container.Attributes[subject], _ = attributeFor(e.Subject())
	}
	if e.Time() != zeroTime {
		container.Attributes[timeAttr], _ = attributeFor(e.Time())
	}
	for name, value := range e.Extensions() {
		attr, err := attributeFor(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode attribute %s: %s", name, err)
		}
		container.Attributes[name] = attr
	}
	container.Data = &cloudevents.CloudEvent_BinaryData{
		BinaryData: e.Data(),
	}
	if e.DataContentType() == ContentTypeProtobuf {
		anymsg := &anypb.Any{
			TypeUrl: e.DataSchema(),
			Value:   e.Data(),
		}
		container.Data = &cloudevents.CloudEvent_ProtoData{
			ProtoData: anymsg,
		}
	}
	return container, nil
}

func attributeFor(v interface{}) (*cloudevents.CloudEvent_CloudEventAttributeValue, error) {
	vv, err := types.Validate(v)
	if err != nil {
		return nil, err
	}
	attr := &cloudevents.CloudEvent_CloudEventAttributeValue{}
	switch vt := vv.(type) {
	case bool:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeBoolean{
			CeBoolean: vt,
		}
	case int32:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeInteger{
			CeInteger: vt,
		}
	case string:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeString{
			CeString: vt,
		}
	case []byte:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeBytes{
			CeBytes: vt,
		}
	case types.URI:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeUri{
			CeUri: vt.String(),
		}
	case types.URIRef:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeUriRef{
			CeUriRef: vt.String(),
		}
	case types.Timestamp:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp{
			CeTimestamp: timestamppb.New(vt.Time),
		}
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", v)
	}
	return attr, nil
}

func FromProto(container *cloudevents.CloudEvent) (*v2.Event, error) {
	e := v2.NewEvent()
	e.SetID(container.Id)
	e.SetSource(container.Source)
	e.SetSpecVersion(container.SpecVersion)
	e.SetType(container.Type)
	// NOTE: There are some issues around missing data content type values that
	// are still unresolved. It is an optional field and if unset then it is
	// implied that the encoding used for the envelope was also used for the
	// data. However, there is no mapping that exists between data content types
	// and the envelope content types. For example, how would this system know
	// that receiving an envelope in application/cloudevents+protobuf know that
	// the implied data content type if missing is application/protobuf.
	//
	// It is also not clear what should happen if the data content type is unset
	// but it is known that the data content type is _not_ the same as the
	// envelope. For example, a JSON encoded data value would be stored within
	// the BinaryData attribute of the protobuf formatted envelope. Protobuf
	// data values, however, are _always_ stored as a protobuf encoded Any type
	// within the ProtoData field. Any use of the BinaryData or TextData fields
	// means the value is _not_ protobuf. If content type is not set then have
	// no way of knowing what the data encoding actually is. Currently, this
	// code does not address this and only loads explicitly set data content
	// type values.
	contentType := ""
	if container.Attributes != nil {
		attr := container.Attributes[datacontenttype]
		if attr != nil {
			if stattr, ok := attr.Attr.(*cloudevents.CloudEvent_CloudEventAttributeValue_CeString); ok {
				contentType = stattr.CeString
			}
		}
	}
	switch dt := container.Data.(type) {
	case *cloudevents.CloudEvent_BinaryData:
		e.DataEncoded = dt.BinaryData
		// NOTE: If we use SetData then the current implementation always sets
		// the Base64 bit to true. Direct assignment appears to be the only way
		// to set non-base64 encoded binary data.
		// if err := e.SetData(contentType, dt.BinaryData); err != nil {
		// 	return nil, fmt.Errorf("failed to convert binary type (%s) data: %s", contentType, err)
		// }
	case *cloudevents.CloudEvent_TextData:
		if err := e.SetData(contentType, dt.TextData); err != nil {
			return nil, fmt.Errorf("failed to convert text type (%s) data: %s", contentType, err)
		}
	case *cloudevents.CloudEvent_ProtoData:
		e.SetDataContentType(ContentTypeProtobuf)
		e.DataEncoded = dt.ProtoData.Value
	}
	for name, value := range container.Attributes {
		v, err := valueFrom(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert attribute %s: %s", name, err)
		}
		switch name {
		case datacontenttype:
			vs, _ := v.(string)
			e.SetDataContentType(vs)
		case dataschema:
			vs, _ := v.(string)
			e.SetDataSchema(vs)
		case subject:
			vs, _ := v.(string)
			e.SetSubject(vs)
		case timeAttr:
			vs, _ := v.(types.Timestamp)
			e.SetTime(vs.Time)
		default:
			e.SetExtension(name, v)
		}
	}
	return &e, nil
}

func valueFrom(attr *cloudevents.CloudEvent_CloudEventAttributeValue) (interface{}, error) {
	var v interface{}
	switch vt := attr.Attr.(type) {
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeBoolean:
		v = vt.CeBoolean
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeInteger:
		v = vt.CeInteger
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeString:
		v = vt.CeString
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeBytes:
		v = vt.CeBytes
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeUri:
		uri, err := url.Parse(vt.CeUri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URI value %s: %s", vt.CeUri, err.Error())
		}
		v = uri
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeUriRef:
		uri, err := url.Parse(vt.CeUriRef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URIRef value %s: %s", vt.CeUriRef, err.Error())
		}
		v = types.URIRef{URL: *uri}
	case *cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp:
		v = vt.CeTimestamp.AsTime()
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", vt)
	}
	return types.Validate(v)
}

func NewIDFromString(id string) (uint64, error) {
	if id == "" {
		return emptyID, ErrEmptyID
	}
	i, err := strconv.ParseUint(id, base, bitSize)
	if err != nil {
		return emptyID, err
	}
	return i, nil
}
