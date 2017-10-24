// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package web

import (
	"config"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine"
	"github.com/labstack/echo/engine/standard"
	mw "github.com/labstack/echo/middleware"
	"github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"proxy/server"
	"github.com/wfxiang08/cyutils/overseer"
)

type ApiServer struct {
	cfg         *config.Config
	proxy       *server.Server
	webUser     string
	webPassword string
	*echo.Echo
	listener    *net.TCPListener
}

func NewApiServer(cfg *config.Config, srv *server.Server, listener *net.TCPListener) (*ApiServer, error) {
	s := new(ApiServer)
	s.cfg = cfg
	s.proxy = srv
	s.webUser = cfg.WebUser
	s.webPassword = cfg.WebPassword
	s.listener = listener

	// 使用Echo服务
	s.Echo = echo.New()
	return s, nil
}

func (s *ApiServer) Run() error {
	rolling_log.Info("Web server running...")
	s.RegisterMiddleware()
	s.RegisterURL()
	std := standard.WithConfig(engine.Config{})
	std.SetHandler(s)

	overseer.HttpServe(s.listener, std)
	return nil
}

// 配置日志, 异常处理，认证
func (s *ApiServer) RegisterMiddleware() {
	s.Use(mw.Recover())
	// s.Use(mw.BasicAuth(s.CheckAuth))
}

func (s *ApiServer) RegisterURL() {
	// 注册URL
	s.Get("/api/v1/nodes/status", s.GetNodesStatus)

	s.Post("/api/v1/nodes/slaves", s.AddOneSlave)
	s.Delete("/api/v1/nodes/slaves", s.DeleteOneSlave)
	s.Put("/api/v1/nodes/slaves/status", s.ChangeSlaveStatus)

	s.Put("/api/v1/nodes/masters/status", s.ChangeMasterStatus)

	s.Get("/api/v1/clients", s.GetClientList)

	s.Get("/api/v1/proxy/status", s.GetProxyStatus)
	s.Put("/api/v1/proxy/status", s.ChangeProxyStatus)

	s.Get("/api/v1/proxy/schema", s.GetProxySchema)

	s.Get("/api/v1/proxy/allow_ips", s.GetAllowIps)
	s.Post("/api/v1/proxy/allow_ips", s.AddAllowIps)
	s.Delete("/api/v1/proxy/allow_ips", s.DelAllowIps)

	s.Get("/api/v1/proxy/black_sqls", s.GetAllBlackSQL)
	s.Post("/api/v1/proxy/black_sqls", s.AddOneBlackSQL)
	s.Delete("/api/v1/proxy/black_sqls", s.DelOneBlackSQL)

	s.Get("/api/v1/proxy/slow_sql/time", s.GetSlowLogTime)
	s.Put("/api/v1/proxy/slow_sql/status", s.SwitchSlowSQL)
	s.Put("/api/v1/proxy/slow_sql/time", s.SetSlowLogTime)

	s.Put("/api/v1/proxy/config/save", s.SaveProxyConfig)
}

func (s *ApiServer) CheckAuth(username, password string) bool {
	if username == s.webUser && password == s.webPassword {
		return true
	}
	return false
}
