package main

import "testing"

func TestSoldOutClassification(t *testing.T) {
	response := result{Status: 409, Body: []byte(`{"error":"SOLD_OUT"}`)}
	var runner runner
	runner.checkOrder(response, product{ID: "earlier-drop", Name: "Flash"}, "buyer")
	if runner.wrong.Load() != 0 {
		t.Fatal("an earlier flash drop may be sold out")
	}
	runner.checkOrder(response, product{ID: "regular", Name: "Seed"}, "buyer")
	if runner.wrong.Load() != 1 {
		t.Fatal("a regular product must not sell out in this profile")
	}
}
