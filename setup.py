from distutils.core import setup
setup(
  name = 'ergoncredit',
  packages = ['ergoncredit'],
  version = '1.4',
  license='MIT',
  description = 'A simple module for ergoncredit environment.',
  author = 'Pedro Borges',
  author_email = 'pedro.borges@ergoncredit.com.br', 
  url = 'https://github.com/ergoncredit/ergoncredit', 
  download_url = 'https://github.com/ergoncredit/ergoncredit/archive/refs/tags/1.4.tar.gz',
  keywords = ['Backoffice', 'Api', 'Ergoncredit'],
  install_requires=[ 
          'requests',
          'browser_cookie3',
          'pandas',
          'sqlalchemy'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers', 
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
