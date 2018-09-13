# @Author: Lei Xu <leonardxu>
# @Date:   2018-09-13T10:10:12-04:00
# @Email:  leonard.xu.thu@gmail.com
# @Filename: build-doc.sh
# @Last modified by:   leonardxu
# @Last modified time: 2018-09-13T11:35:20-04:00

cd docs
make clean
cd ..
sphinx-apidoc -o docs/_modules trane
cd docs
make html
