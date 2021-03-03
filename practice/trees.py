class TreeNode():
    def __init__(self, role):
        self.role = role
        self.children = []
        self.parent = None

    def add_child(self, child):
        child.parent = self
        self.children.append(child)

    def print_tree(self):
        print(self.role)
        if self.children:
            for child in self.children:
                child.print_tree()


CEO = TreeNode('CEO')

director1 = TreeNode('Director 1')
director1.add_child(TreeNode('Manager 1'))
director1.add_child(TreeNode('Manager 2'))


director2 = TreeNode('Director 2')
director2.add_child(TreeNode('Manager 1'))
director2.add_child(TreeNode('Manager 2'))

CEO.add_child(director1)
CEO.add_child(director2)

# CEO.print_tree()


class TreeNode():
    def __init__(self, location_granularity):
        self.location_granularity = location_granularity
        self.children = []
        self.parent = None

    def add_child(self, child):
        self.children.append(child)
        child.parent = self
    
    def print_tree(self):
        print(self.location_granularity)
        for child in self.children:
            child.print_tree()


Global = TreeNode('Global')

India = TreeNode('India')
India.add_child(TreeNode('Calcuta'))
India.add_child(TreeNode('Delhi'))

US = TreeNode('USA')
US.add_child(TreeNode('California'))
US.add_child(TreeNode('New Jersey'))

Global.add_child(India)
Global.add_child(US)

Global.print_tree()