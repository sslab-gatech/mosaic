Ring buffer over PCI bus
========================

How to git
----------
- Add this repository to your project repository
~~~~~{.sh}
git subtree add --squash git@tc.gtisc.gatech.edu:pci-ring-buffer master -P [your local dir.]/pci-ring-buffer
~~~~~

- Push your local changes of to this master
~~~~~{.sh}
git subtree push --prefix [your local dir.]/pci-ring-buffer git@tc.gtisc.gatech.edu:pci-ring-buffer master --squash
~~~~~

- Pull the latest from this master
~~~~~{.sh}
git subtree pull --prefix [your local dir.]/pci-ring-buffer git@tc.gtisc.gatech.edu:pci-ring-buffer master --squash
~~~~~

- References
    - [The power of Git subtree](https://developer.atlassian.com/blog/2015/05/the-power-of-git-subtree/)