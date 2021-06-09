use Mix.Config
config :ex_aws,
       json_codec: Jason,
       debug_requests: true,
       access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, :instance_role],
       secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, :instance_role],
       security_token: [{:system, "AWS_SESSION_TOKEN"}, :instance_role],
       region: "ap-south-1"
