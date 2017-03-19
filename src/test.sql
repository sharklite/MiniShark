/** MS SQL **/

CREATE TABLE [person](
	[id] [int] IDENTITY(1,1) PRIMARY KEY,
	[name] [varchar](20) NULL,
	[age] [int] NULL,
	[birthDay] [datetime] NULL,
	[times] [timestamp] NOT NULL,
	[dat] [binary](50) NULL,
)

