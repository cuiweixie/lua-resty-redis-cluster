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


no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: hmset key-pairs
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
            local res, err = red:hmset("animals", "dog{key}", "bark", "cat{key}", "meow")
            if not res then
                ngx.say("failed to set animals: ", err)
                return
            end
            ngx.say("hmset animals: ", res)

            local res, err = red:hmget("animals", "dog{key}", "cat{key}")
            if not res then
                ngx.say("failed to get animals: ", err)
                return
            end

            ngx.say("hmget animals: ", res)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
hmset animals: OK
hmget animals: barkmeow
--- no_error_log
[error]





=== TEST 2: hmset a single scalar
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

            local res, err = red:hmset("animals", "cat","cat")
            if not res then
                ngx.say("failed to set animals: ", err)
                return
            end
            ngx.say("hmset animals: ", res)

            local res, err = red:hmget("animals", "cat")
            if not res then
                ngx.say("failed to get animals: ", err)
                return
            end

            ngx.say("hmget animals: ", res)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
hmset animals: OK
hmget animals: cat
--- no_error_log
[error]

