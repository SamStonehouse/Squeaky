class { 'nodejs':
	version => 'stable',
}

package { 'n':
  provider => npm
}

package { 'nodemon':
  provider => npm
}

include evedb