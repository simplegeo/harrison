srcs = FileList['lib/**/*.js']
tests = FileList['test/**/*.test.js']

doc_srcs = srcs

JSDOC_TEMPLATE = "/usr/local/Cellar/jsdoc-toolkit/2.3.2/libexec/jsdoc-toolkit/templates/jsdoc"
#JSDOC_TEMPLATE = "#{ENV['HOME']}/.jsdoc/codeview"
JSDOC = "jsdoc -t=#{JSDOC_TEMPLATE}"

task :doc do
    sh "#{JSDOC} -d=doc #{doc_srcs}"
end

task :test do
    sh "env NODE_PATH=lib expresso #{tests}"
end

task :test_cov do
    sh "env NODE_PATH=lib expresso #{tests} --cov 2>&1 | less -R"
end

task :tags do
    sh "ctags #{srcs}"
end
