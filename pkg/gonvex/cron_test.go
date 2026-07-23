package gonvex

import (
	"reflect"
	"testing"
)

func TestAppSupportsTenantCronRegistration(t *testing.T) {
	appType := reflect.TypeOf(NewApp())
	if _, ok := appType.MethodByName("TenantCron"); !ok {
		t.Fatal("App is missing TenantCron")
	}
	if _, ok := reflect.TypeOf(CronSpec{}).FieldByName("PerTenant"); !ok {
		t.Fatal("CronSpec is missing the per-tenant dispatch marker")
	}
}
