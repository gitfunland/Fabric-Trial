[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4c10fb2e-638f-4197-a8f2-b0ae64c73592"]}[Data]{[lakehouseId = "69071d2d-c649-43fa-b728-6a0b78864a38"]}[Data];
[BindToDefaultDestination = true]
shared dimension_customer = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4c10fb2e-638f-4197-a8f2-b0ae64c73592"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "69071d2d-c649-43fa-b728-6a0b78864a38"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "dimension_customer.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 11, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"CustomerKey", Int64.Type}, {"WWICustomerID", Int64.Type}, {"Customer", type text}, {"BillToCustomer", type text}, {"Category", type text}, {"BuyingGroup", type text}, {"PrimaryContact", type text}, {"PostalCode", type text}, {"ValidFrom", type datetime}, {"ValidTo", type datetime}, {"LineageKey", Int64.Type}})
in
  #"Changed column type";
