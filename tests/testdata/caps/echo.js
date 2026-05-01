// pepper:name = echo
// pepper:groups = default
// pepper:version = 1.0.0

function run(inputs) {
    return { ...inputs, echoed: true };
}

module.exports = { run };
