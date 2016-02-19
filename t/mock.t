# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;/usr/local/openresty-dev/lualib/?.lua;";
    lua_package_cpath "/usr/local/openresty-dev/lualib/?.so;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_REDIS_PORT} ||= 6379;

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: continue using the obj when read timeout happens
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "test",
                serv_list = {
                    {ip="127.0.0.1", port = 3100},
                    {ip="127.0.0.1", port = 3101},
                    {ip="127.0.0.1", port = 3102},
                    {ip="127.0.0.1", port = 3200},
                    {ip="127.0.0.1", port = 3201},
                    {ip="127.0.0.1", port = 3202},
                },
            }
            local redis_cluster = require "resty.rediscluster"
            local red = redis_cluster:new(config)

            red:set_timeout(1) -- 0.1 sec

            for i = 1, 2 do
                local data, err = red:get("foo_xx")
                if not data or data == ngx.null then
                    ngx.say("failed to get: foo_xx")
                else
                    ngx.say("get: ", data);
                end
                ngx.sleep(0.1)
            end

            red:close()
        ';
    }
--- request
GET /t
--- response_body
failed to get: foo_xx
failed to get: foo_xx
--- no_error_log


